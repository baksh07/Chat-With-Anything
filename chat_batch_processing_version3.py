from kafka import KafkaConsumer, KafkaProducer
from send_post import LingoVoiceProcessor
import json
import signal
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
from logger_config import setup_logger
from settings import Settings

settings = Settings.from_environment()
logger = setup_logger(__name__, "chat_data_ingestion_pipeline.log")


class KafkaLingoBridge:
    def __init__(self):
        self.running = True
        self.kafka_config = {
            'bootstrap_servers': settings.kafka_bootstrap_servers,
            'input_topic': settings.kafka_input_topic,
            'output_topic': settings.kafka_output_topic
        }
        self.load_data = LoadData()
        self.consumer = None
        self.producer = None
        self.upload_dir = str(settings.upload_dir)
        self.chroma_persist_dir = str(settings.chroma_persist_dir)
        self.processor = LingoVoiceProcessor(
            settings.processor_audio_url,
            settings.processor_text_url,
        )
        self.scraped_json_path = str(settings.scraped_json_dir)
        # LLM client for summarization
        self.llm_client = OpenAI(
            api_key=settings.summary_llm_api_key,
            base_url=settings.summary_llm_base_url,
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
                group_id='chat-group',
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
        try:
            print(100*"=")
            print("Processing message:", message)
            print(100*"=")
            file_url = message.get('audio_url')  # This actually contains the file URL
            metadata = message.get('metadata')
            unique_id = metadata["uniqueId"]
            file_type = metadata["file_type"].lower()
            title = None  # Initialize title variable
            
            logger.info(f"Processing message with unique ID: {unique_id}")

            if file_type in ['mp3', 'link', 'text', "wav", "m4a", "flac", "aac", "ogg", "webm", "wma", "alac", "opus" ]:
                pass
            else: 
            # if file_type != "mp3" or file_type != "link" or file_type != "text":
                # Download the file first
                date = datetime.today().strftime("%Y-%m-%d")
                filename = f"{unique_id}_{date}.{file_type}"
                file_path = f"{self.upload_dir}/{filename}"
                
                logger.info(f"Downloading {file_type} file from {file_url}")
                response = requests.get(file_url)
                with open(file_path, 'wb') as f:
                    f.write(response.content)
                logger.info(f"Saved file to: {file_path}")
            
            # Process the file based on its type
            text = ""
            
            if file_type == "pdf":
                text = self.load_data.load_pdf(file_path)
                
            elif file_type in ["ppt", "pptx"]:

                text = self.load_data.load_ppt(file_path)
                
            elif file_type in ["doc", "docx"]:
                if file_type == "doc":
                    #logging
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
                text_file = f"{self.upload_dir}/{unique_id}.txt"
                with open(text_file, "w", encoding="utf-8") as file:
                    file.write(file_url)
                text = self.load_data.load_text(text_file)
                
            elif file_type == "mp3" or file_type == "wav" or file_type == "m4a" or file_type == "ogg" or file_type == "flac" or file_type == "opus" or file_type == "aac" or file_type == "amr":                                                                                                    
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

                self.producer.send(self.kafka_config['output_topic'], response_payload)
                logger.info(f"Published result to {self.kafka_config['output_topic']}")

            elif file_type == "link":
                if not file_url:
                    raise ValueError("Missing 'audio_url' in the message.")
                logger.info(f"Processing web link: {file_url}")
                scrape_result_old = self.scraper.scrape(file_url)
                path = f"{self.scraped_json_path}/{unique_id}.json"
                
                if os.path.exists(path):
                    logger.info(f"Scraped JSON already exists at {path}, loading existing data.")
                    scraped_json = self.load_json_to_dict(path)
                    text = scraped_json[0]['text']
                    logger.info(f"Extracted text from web link: {text[:100]}...")  # Log first 100 chars
                else:
                    logger.info(f"Scraped JSON does not exist at {path}, scraping the web link.")
                    scrape_result = scrape_with_cli(file_url, id=unique_id)
                    logger.info(f"Scrape result: {scrape_result}")
                    scraped_json = self.load_json_to_dict(path)
                    text = scraped_json[0]['text']
                    logger.info(f"Extracted text from web link: {text[:100]}...")  # Log first 100 chars

                # for item in scrape_result:
                #     text = final_output + " " + item.get("text")
                if not text:
                    raise ValueError("No text extracted from the web link.")
                TEXT2 , title = scrape_result_old
                if metadata is not None:
                    metadata["title"] = title
                
            else:
                logger.warning(f"Unsupported file type: {file_type}")
                return
            
            try:
                self.save_to_chroma(text, collection_name=unique_id, metadata=metadata)
                logger.info(f"Successfully saved text to Chroma DB for unique ID: {unique_id}")
                if file_type in ["pdf", "ppt", "pptx", "doc", "docx", "txt", "link", "text"]:
                    text_for_summary = text[:2000]
                    result = self.processor.process_text(text_for_summary)
                    # Only set title if it exists
                    if title is not None:
                        result['response']['results']['title'] = title
                    response_payload = {
                        "input_file_url": file_url,
                        "status": "success",
                        "result": result,
                        "metadata": metadata
                    }
                    self.producer.send(self.kafka_config['output_topic'], response_payload)
                    logger.info(f"Published result to {self.kafka_config['output_topic']}")
            except Exception as e:
                logger.error(f"Error saving to Chroma DB: {str(e)}")
                # Optionally send an error response
                error_payload = {
                    "input_file_url": file_url,
                    "status": "failed",
                    "result": str(e),
                    "metadata": metadata
                }
                self.producer.send(self.kafka_config['output_topic'], error_payload)
                

        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            # Optionally send an error response
            error_payload = {
                "error": str(e),
              "metadata": metadata,
                "status": "error"
            }
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