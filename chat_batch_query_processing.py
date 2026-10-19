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
from pathlib import Path
from psycopg2 import pool
from psycopg2 import sql
import psycopg2
from langchain.retrievers import MultiVectorRetriever
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KafkaLingoBridge:
    def __init__(self):
        self.openai_client = OpenAI(
            base_url="https://api.groq.com/openai/v1",
            api_key=os.environ["GROQ_API_KEY"]
        )
        self.running = True
        self.kafka_config = {
            'bootstrap_servers': ['216.48.183.140:9092'],
            'input_topic': 'chat-input-query',
            'output_topic': 'chat_ai_output_query'
        }
        self.load_data = LoadData()
        self.consumer = None
        self.producer = None
        project_dir = Path(__file__).resolve().parent
        self.upload_dir = str(project_dir / "UPLOAD")
        self.chroma_persist_dir = str(project_dir / "chroma_db")  # Directory to persist Chroma DB
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
        self.db_pool = pool.SimpleConnectionPool(
            1, 10,  # Min and max connections
            dbname='Unifile_Chat',
            user="slaif",
            password="DEV!n$tanc3",
            host='91.203.132.196',
            port=5432
        )



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

    # Database connection function
    def connect_to_db(self):
        try:
            return self.db_pool.getconn()
        except Exception as e:
            print(f"Database connection error: {e}")
            return None
        
    def release_db_connection(self, conn):
        if conn:
            self.db_pool.putconn(conn)


    def extract_chathistory(self, chat_id):
        conn = self.connect_to_db()
        if not conn:
            return None
        try:
            with conn.cursor() as cursor:
                if isinstance(chat_id, list):
                    query = sql.SQL("SELECT * FROM sl_schema.message WHERE chat_id IN ({})").format(
                        sql.SQL(', ').join(sql.Placeholder() * len(chat_id))
                    )
                    cursor.execute(query, tuple(chat_id))
                else:
                    query = "SELECT * FROM sl_schema.message WHERE chat_id = %s"
                    cursor.execute(query, (chat_id,))

                his = cursor.fetchall()
                return his[-10:]
        except psycopg2.Error as e:
            print(f"Database error: {e}")
            return None
        finally:
            self.release_db_connection(conn)

    def process_message(self, message):
        try:
            print(100*"=")
            print("Processing message:", message)
            print(100*"=")
            query = message.get('query')  # This actually contains the file URL
            chat_id = message.get('chat_id')
            uniqueId = message.get('uniqueId')


            #get chat history
            chat_history = self.extract_chathistory(chat_id)
            
            retrievers = []
            for uid in uniqueId:
                try:
                    vectorstore = Chroma(
                        collection_name=uid,
                        persist_directory=self.chroma_persist_dir,
                        embedding_function=self.embeddings
                    )
                    retrievers.append(vectorstore.as_retriever())
                    logger.info(f"Successfully loaded retriever for collection: {uid}")
                except Exception as e:
                    logger.error(f"Error loading collection {uid}: {str(e)}")
                    continue
            
            # Create a multi-vector retriever that combines all retrievers
            if not retrievers:
                raise ValueError("No valid retrievers could be created from the provided unique IDs")
                
            mretriever = MultiVectorRetriever(retrievers=retrievers)
            # Retrieve relevant documents
            db_res = mretriever.get_relevant_documents(query)
            
            # Format the retrieved documents
            formatted_db_res = "\n".join([doc.page_content for doc in db_res])

            # Create the prompt with all components
            prompt = f"""
            INPUT DATA:

            Database Query Results: {formatted_db_res}
            Chat History: {chat_history}
            User Query: {query}

            RESPONSE FORMAT:
            Deliver a single, well-structured paragraph answering the query 
            Include essential information without technical jargon
            Maintain clarity and conciseness
            Avoid source citations or qualifiers
            Use professional yet warm tone

            Transition naturally to a relevant follow-up question
            Show genuine interest in user's needs
            Keep the follow-up contextual and engaging
            Avoid mechanical or overly formal phrasing

            INSTRUCTIONS: 
            -Do not use phrases like: 'here is the reponse:' etc to give your answer.
            -Give one followup question only. not more.
            """    

            stream = self.openai_client.chat.completions.create(
                    messages=[
                        {
                            "role": "system",
                            "content": prompt
                        },
                        {
                            "role": "user",
                            "content": f"{query}"
                        }
                    ],
                    model="llama-3.3-70b-versatile",

                )
            full_response = stream.choices[0].message.content

            print(f"Reply: {full_response}")
            response_payload = {
                    "query": query,
                    "status": "success",
                    "result": full_response,
                    "metadata": "metadata"
                }
            self.producer.send(self.kafka_config['output_topic'], response_payload)
            logger.info(f"Published result to {self.kafka_config['output_topic']}")

        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            # Optionally send an error response
            error_payload = {
                "error": str(e),
                "metadata": "metadata",
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