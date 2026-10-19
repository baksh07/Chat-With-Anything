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
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
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
        self.chroma_persist_dir = "/mnt/data2/vocally_batch/chroma_db"  # Directory to persist Chroma DB
        self.processor = LingoVoiceProcessor("http://216.48.184.150:5999/LingoVoice/process_audio_url/")
        
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
            
            logger.info(f"Processing message with unique ID: {unique_id}")
            
            if file_type != "audio":
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
                text = self.load_data.load_word(file_path)
                
            elif file_type == "txt":
                text = self.load_data.load_text(file_path)
                
            elif file_type == "audio":
                if not file_url:
                    raise ValueError("Missing 'audio_url' in the message.")

                logger.info(f"Processing audio URL: {file_url}")
                result = self.processor.process_audio_url(file_url)
                text = result["result"]["response"]["results"]["transcription"]
                response_payload = {
                    "input_audio_url": file_url,
                    "status": "success",
                    "result": result,
                    "metadata": metadata
                }

                self.producer.send(self.kafka_config['output_topic'], response_payload)
                logger.info(f"Published result to {self.kafka_config['output_topic']}")
            else:
                logger.warning(f"Unsupported file type: {file_type}")
                return
            
            try:
                self.save_to_chroma(text, collection_name=unique_id, metadata=metadata)
                logger.info(f"Successfully saved text to Chroma DB for unique ID: {unique_id}")
                if file_type in ["pdf", "ppt", "pptx", "doc", "docx", "txt"]:
                    response_payload = {
                        "input_file_url": file_url,
                        "status": "success",
                        "result": "",
                        "metadata": metadata
                    }
                    self.producer.send(self.kafka_config['output_topic'], response_payload)
                    logger.info(f"Published result to {self.kafka_config['output_topic']}")
            except Exception as e:
                logger.error(f"Error saving to Chroma DB: {str(e)}")
                # Optionally send an error response
                response_payload = {
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