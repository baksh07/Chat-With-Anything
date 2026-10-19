from kafka import KafkaConsumer, KafkaProducer
from send_post import LingoVoiceProcessor
import json
import logging
import signal
import sys
from datetime import datetime
import requests
from data_ingestion_pipeline import LoadData
from typing import Optional, List
from langchain.vectorstores import Chroma
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.docstore.document import Document as LangchainDocument
from langchain.docstore import document
import os
from openai import OpenAI
from spire.doc import *
from spire.doc.common import *
from SERVICES.web_scrape_service import WebScraper
from myproject.runner import scrape_with_cli

# Configure logging
log_file_path = "logs/vocally_data_ingestion_pipeline.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler(sys.stdout)  # Keeps printing to console as well
    ]
)
logger = logging.getLogger(__name__)


class KafkaLingoBridge:
    def __init__(self):
        self.running = True
        self.kafka_config = {
            'bootstrap_servers': ['216.48.183.140:9092'],
            'input_topic': 'vocally_input',
            'output_topic': 'vocally_ai_output'
        }
        self.load_data = LoadData()
        self.consumer = None
        self.producer = None
        self.upload_dir = "/mnt/data2/vocally_batch/UPLOAD"
        self.chroma_persist_dir = "/mnt/data2/vocally_batch/chroma_db"
        self.processor = LingoVoiceProcessor("http://216.48.184.150:5999/LingoVoice/process_audio_url/")
        self.scraped_json_path = "/mnt/data2/vocally_batch/myproject"
        
        # Track processed messages to avoid duplicates
        self.processed_messages = set()
        
        # LLM client for summarization
        self.llm_client = OpenAI(
            api_key="sandlogic-llm",
            base_url="http://164.52.196.111:3005"  # Replace with actual LLM URL
        )
        self.document = Document()
        
        # Initialize embeddings model
        self.embeddings = HuggingFaceEmbeddings(
            model_name="sentence-transformers/all-MiniLM-L6-v2",
            model_kwargs={'device': 'cpu'}
        )
        
        # Initialize text splitter
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000,
            chunk_overlap=200
        )
        self.scraper = WebScraper(log_level='DEBUG')

    def load_json_to_dict(self, file_path: str) -> dict:
        """
        Reads a JSON file from the given path and returns it as a Python dict.
        """
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data

    def get_summary(self, text: str) -> str:
        """Get summary of the text using LLM"""
        try:
            # Take first 2 pages worth of text (approx 2000 chars)
            text_for_summary = text[:2000]
            
            prompt = [
                {
                    "role": "user",
                    "content": f"""Please provide a concise summary of the following text, highlighting the key points and main ideas in a clear and structured manner. 
                    The summary should retain the essence of the content while avoiding unnecessary details or repetitions.
                    Strictly only provide the Summary, Do not give any texts like "Here is the summary".
                    
                    Text to summarize: {text_for_summary}"""
                }
            ]
            
            chat_completion = self.llm_client.chat.completions.create(
                messages=prompt,
                model="sl-llm",
                stream=False,
                temperature=0.3  # Lower temperature for more focused summaries
            )
            
            summary = chat_completion.choices[0].message.content
            logger.info("Successfully generated summary")
            return summary
        except Exception as e:
            logger.error(f"Error generating summary: {str(e)}")
            return ""

    def save_to_chroma(self, text: str, collection_name: str, metadata: dict = None):
        """
        Save extracted text to Chroma DB with the given collection name
        
        Args:
            text: Extracted text content
            collection_name: Name of the collection (will use uniqueId)
            metadata: Additional metadata to store with the documents
        """
        try:
            # Create directory if it doesn't exist
            os.makedirs(self.chroma_persist_dir, exist_ok=True)
            
            # Split the text into chunks
            texts = self.text_splitter.split_text(text)
            
            # Create documents with metadata
            docs = [LangchainDocument(page_content=text, metadata=metadata or {}) 
                   for text in texts]
            
            # Create or load the collection
            chroma_db = Chroma.from_documents(
                documents=docs,
                embedding=self.embeddings,
                collection_name=collection_name,
                persist_directory=self.chroma_persist_dir
            )
            
            # Persist the database
            chroma_db.persist()
            logger.info(f"Successfully saved documents to Chroma DB collection: {collection_name}")
            
            return True
        except Exception as e:
            logger.error(f"Error saving to Chroma DB: {str(e)}")
            return False

    def initialize(self):
        try:
            self.consumer = KafkaConsumer(
                self.kafka_config['input_topic'],
                bootstrap_servers=self.kafka_config['bootstrap_servers'],
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='vocally-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                # Add these configurations to prevent duplicate processing
                max_poll_records=1,  # Process one message at a time
                session_timeout_ms=30000,
                heartbeat_interval_ms=3000
            )
            
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_config['bootstrap_servers'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                # Add these configurations for better reliability
                acks='all',  # Wait for all replicas to acknowledge
                retries=3,
                max_in_flight_requests_per_connection=1  # Ensure ordering
            )
            
            logger.info("Kafka consumer and producer initialized successfully.")
        except Exception as e:
            logger.error(f"Initialization failed: {str(e)}")
            raise

    def shutdown(self):
        logger.info("Shutting down Kafka service...")
        self.running = False
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.flush()
            self.producer.close()
        logger.info("Shutdown complete.")

    def publish_result(self, payload: dict, unique_id: str):
        """
        Centralized method to publish results to Kafka topic
        Prevents duplicate publishing by checking if already published
        """
        try:
            # Check if this unique_id has already been processed
            if unique_id in self.processed_messages:
                logger.warning(f"Message with unique_id {unique_id} already processed, skipping publication")
                return False
            
            # Publish to Kafka
            future = self.producer.send(self.kafka_config['output_topic'], payload)
            
            # Wait for the message to be sent successfully
            record_metadata = future.get(timeout=10)
            
            # Mark as processed only after successful publication
            self.processed_messages.add(unique_id)
            
            logger.info(f"Successfully published result for unique_id: {unique_id} to topic: {record_metadata.topic}, partition: {record_metadata.partition}, offset: {record_metadata.offset}")
            return True
            
        except Exception as e:
            logger.error(f"Error publishing result for unique_id {unique_id}: {str(e)}")
            return False

    def process_message(self, message):
        try:
            print("=" * 100)
            print("Processing message:", message)
            print("=" * 100)

            file_url = message.get('audio_url')
            metadata = message.get('metadata', {})
            unique_id = metadata.get("uniqueId")
            file_type = metadata.get("file_type", "").lower()
            title = None

            # Validate required fields
            if not unique_id:
                logger.error("Missing uniqueId in metadata")
                return
            
            if not file_url:
                logger.error(f"Missing audio_url for unique_id: {unique_id}")
                return

            logger.info(f"Processing message with unique ID: {unique_id}")

            # Check if this message has already been processed
            if unique_id in self.processed_messages:
                logger.warning(f"Message with unique_id {unique_id} already processed, skipping")
                return

            document_types = ["pdf", "ppt", "pptx", "doc", "docx", "txt", "text"]
            audio_types = ["mp3", "wav", "m4a", "ogg", "flac", "opus", "aac", "amr", "wma", "webm", "alac"]
            text = ""

            # If it's not audio/link/text type, we assume it's a file to download
            if file_type not in audio_types + ["link", "text"]:
                date = datetime.today().strftime("%Y-%m-%d")
                filename = f"{unique_id}_{date}.{file_type}"
                file_path = os.path.join(self.upload_dir, filename)

                logger.info(f"Downloading {file_type} file from {file_url}")
                response = requests.get(file_url)
                response.raise_for_status()  # Raise an exception for bad status codes
                
                with open(file_path, 'wb') as f:
                    f.write(response.content)
                logger.info(f"Saved file to: {file_path}")

            # ========== TEXT/FILE HANDLING ==========
            if file_type == "pdf":
                text = self.load_data.load_pdf(file_path)

            elif file_type in ["ppt", "pptx"]:
                text = self.load_data.load_ppt(file_path)

            elif file_type in ["doc", "docx"]:
                if file_type == "doc":
                    logger.info(f"Converting DOC to DOCX for file: {file_path}")
                    self.document.LoadFromFile(file_path)
                    new_file_path = file_path.replace('.doc', '.docx')
                    self.document.SaveToFile(new_file_path, FileFormat.Docx2016)
                    text = self.load_data.load_word(new_file_path)
                else:
                    text = self.load_data.load_word(file_path)

            elif file_type == "txt":
                text = self.load_data.load_text(file_path)

            elif file_type == "text":
                text_file = os.path.join(self.upload_dir, f"{unique_id}.txt")
                with open(text_file, "w", encoding="utf-8") as file:
                    file.write(file_url)
                text = self.load_data.load_text(text_file)

            elif file_type in audio_types:
                logger.info(f"Processing audio URL: {file_url}")
                result = self.processor.process_audio_url(file_url)
                text = result['response']['results']['transcription']
                # Validate extracted text
                if not text or not text.strip():
                    logger.warning(f"No transcription text extracted for unique_id: {unique_id}")
                    return
                
                # ========== SAVE TO CHROMA FIRST ==========
                saved = self.save_to_chroma(text, collection_name=unique_id, metadata=metadata)
                if saved:
                    logger.info(f"Audio transcription saved to Chroma DB for unique ID: {unique_id}")
                else:
                    logger.error(f"Failed to save audio transcription to Chroma DB for unique ID: {unique_id}")
     
                response_payload = {
                    "input_audio_url": file_url,
                    "status": "success",
                    "result": result,
                    "metadata": metadata
                }

                # Use centralized publish method
                self.publish_result(response_payload, unique_id)


            elif file_type == "link":
                try:
                    logger.info(f"Processing web link: {file_url}")
                    TEXT2, title = self.scraper.scrape(file_url)
                    json_path = os.path.join(self.scraped_json_path, f"{unique_id}.json")

                    if os.path.exists(json_path):
                        logger.info(f"Scraped JSON already exists at {json_path}, loading it.")
                        scraped_json = self.load_json_to_dict(json_path)
                    else:
                        logger.info(f"Scraped JSON not found at {json_path}, running CLI scraper.")
                        scrape_with_cli(file_url, id=unique_id)
                        scraped_json = self.load_json_to_dict(json_path)
                    if len(scraped_json) == 0:
                        text = TEXT2
                    else:
                        text = scraped_json[0]['text']               
                    if metadata is not None:
                        metadata["title"] = title
                except Exception as e:
                    logger.error(f"Error processing web link {file_url}: {str(e)}")
                    text = TEXT2
                    

            else:
                logger.warning(f"Unsupported file type: {file_type}")
                return

            # Validate extracted text
            if not text or not text.strip():
                logger.warning(f"No text extracted for unique_id: {unique_id}")
                return

            # ========== SAVE TO CHROMA ==========
            saved = self.save_to_chroma(text, collection_name=unique_id, metadata=metadata)
            if saved:
                logger.info(f"Text saved to Chroma DB for unique ID: {unique_id}")

            # ========== TEXT RESULT PUBLISH ==========
            if file_type in document_types + ["link", "text"]:
                text_for_summary = text[:2000]
                result = self.processor.process_text(text_for_summary)

                if title is not None:
                    result['response']['results']['title'] = title

                response_payload = {
                    "input_file_url": file_url,
                    "status": "success",
                    "result": result,
                    "metadata": metadata
                }

                # Use centralized publish method
                self.publish_result(response_payload, unique_id)

        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            error_payload = {
                "error": str(e),
                "metadata": metadata,
                "status": "error"
            }
            # Only publish error if we have a valid unique_id and haven't processed it
            if unique_id and unique_id not in self.processed_messages:
                self.publish_result(error_payload, unique_id)

    def run(self):
        try:
            self.initialize()
            logger.info("Listening for messages...")

            while self.running:
                message_batch = self.consumer.poll(timeout_ms=1000)
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        if not self.running:
                            break
                        try:
                            self.process_message(message.value)
                            # Commit the offset after successful processing
                            # self.consumer.commit()
                        except Exception as e:
                            logger.error(f"Error processing message: {str(e)}")
                            # Don't commit the offset if processing failed

        except Exception as e:
            logger.error(f"Fatal error: {str(e)}")
        finally:
            self.shutdown()


def setup_signal_handlers(service):
    def handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        service.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


if __name__ == "__main__":
    bridge = KafkaLingoBridge()
    setup_signal_handlers(bridge)
    bridge.run()