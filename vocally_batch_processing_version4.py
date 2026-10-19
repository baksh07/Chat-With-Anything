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
from pathlib import Path
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
        project_dir = Path(__file__).resolve().parent
        self.upload_dir = str(project_dir / "UPLOAD")
        self.chroma_persist_dir = str(project_dir / "chroma_db")
        self.processor = LingoVoiceProcessor("http://216.48.184.150:5999/LingoVoice/process_audio_url/")
        self.scraped_json_path = "/mnt/data2/vocally_batch/myproject" 
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
            
            # Get summary of the text
            # summary = self.get_summary(text)
            # print(50*"=")
            # print("Generated Summary:", summary)
            # print(50*"=")
            # if summary:
            #     # Add summary to metadata
            #     if metadata is None:
            #         metadata = {}
            #     metadata['summary'] = summary
            
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
                value_deserializer=lambda x: json.loads(x.decode('utf-8')))
            
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_config['bootstrap_servers'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'))
            
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

    def process_message(self, message):
        already_published = False

        try:
            print("=" * 100)
            print("Processing message:", message)
            print("=" * 100)

            file_url = message.get('audio_url')
            metadata = message.get('metadata')
            unique_id = metadata.get("uniqueId")
            file_type = metadata.get("file_type", "").lower()
            title = None

            logger.info(f"Processing message with unique ID: {unique_id}")

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
                if not file_url:
                    raise ValueError("Missing 'audio_url' in the message.")
                logger.info(f"Processing audio URL: {file_url}")
                result = self.processor.process_audio_url(file_url)
                text = result['response']['results']['transcription']

                response_payload = {
                    "input_audio_url": file_url,
                    "status": "success",
                    "result": result,
                    "metadata": metadata
                }

                if not already_published:
                    self.producer.send(self.kafka_config['output_topic'], response_payload)
                    already_published = True
                    logger.info(f"Published result to {self.kafka_config['output_topic']}")

            elif file_type == "link":
                if not file_url:
                    raise ValueError("Missing 'audio_url' in the message.")

                logger.info(f"Processing web link: {file_url}")
                scrape_result_old = self.scraper.scrape(file_url)
                json_path = os.path.join(self.scraped_json_path, f"{unique_id}.json")

                if os.path.exists(json_path):
                    logger.info(f"Scraped JSON already exists at {json_path}, loading it.")
                    scraped_json = self.load_json_to_dict(json_path)
                else:
                    logger.info(f"Scraped JSON not found at {json_path}, running CLI scraper.")
                    scrape_with_cli(file_url, id=unique_id)
                    scraped_json = self.load_json_to_dict(json_path)

                text = scraped_json[0]['text']
                if not text:
                    raise ValueError("No text extracted from the web link.")

                TEXT2, title = scrape_result_old
                if metadata is not None:
                    metadata["title"] = title

            else:
                logger.warning(f"Unsupported file type: {file_type}")
                return

            # ========== SAVE TO CHROMA ==========
            saved = self.save_to_chroma(text, collection_name=unique_id, metadata=metadata)
            if saved:
                logger.info(f"Text saved to Chroma DB for unique ID: {unique_id}")

            # ========== TEXT RESULT PUBLISH ==========
            if not already_published and file_type in document_types + ["link", "text"]:
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

                self.producer.send(self.kafka_config['output_topic'], response_payload)
                already_published = True
                logger.info(f"Published result to {self.kafka_config['output_topic']}")

        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            error_payload = {
                "error": str(e),
                "metadata": metadata,
                "status": "error"
            }
            if not already_published:
                self.producer.send(self.kafka_config['output_topic'], error_payload)


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
                        self.process_message(message.value)

        except Exception as e:
            logger.error(f"Fatal error: {str(e)}")
        finally:
            self.shutdown()


def setup_signal_handlers(service):
    def handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        service.shutdown()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


if __name__ == "__main__":
    bridge = KafkaLingoBridge()
    setup_signal_handlers(bridge)
    bridge.run()