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
from langchain.retrievers import MultiVectorRetriever, EnsembleRetriever

from openai import OpenAI
from logger_config import setup_logger





class VocallyChatPipeline:
    def __init__(self):
        self.openai_client = OpenAI(
            api_key="sandlogic",
            base_url="http://216.48.182.241:3535/v1",  
        )
        self.logger = setup_logger("VocallyChatPipeline")
        self.logger.info("loading data")
        self.load_data = LoadData()
        self.chroma_persist_dir = str(Path(__file__).resolve().parent / "chroma_db")
        self.logger.info(f"Chroma persist directory set to: {self.chroma_persist_dir}")
        self.embeddings = HuggingFaceEmbeddings(
            model_name="sentence-transformers/all-MiniLM-L6-v2",
            model_kwargs={'device': 'cpu'}
        )
        self.logger.info("HuggingFace embeddings initialized.")
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000,
            chunk_overlap=200
        )
        self.db_pool = pool.SimpleConnectionPool(
            1, 10,  # Min and max connections
            dbname='vocally',
            user="slaif",
            password="DEV!n$tanc3",
            host='91.203.132.196',
            port=5432
        )
        self.logger.info("Database connection pool initialized.")

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
            # print(100*"=")
            # print("Processing message:", message)
            # print(100*"=")
            self.logger.info(f"Processing message: {message}")
            message = message[0]
            query = message.get('query')  # This actually contains the file URL
            chat_id = message.get('chat_id')
            uniqueId = message.get('uniqueId')


            #get chat history
            chat_history = self.extract_chathistory(chat_id)
            self.logger.info(f"retreived chat history: {chat_history}")
            retrievers = []
            for uid in uniqueId:
                try:
                    vectorstore = Chroma(
                        collection_name=uid,
                        persist_directory=self.chroma_persist_dir,
                        embedding_function=self.embeddings
                    )
                    retrievers.append(vectorstore.as_retriever())
                    self.logger.info(f"Successfully loaded retriever for collection: {uid}")
                    # print(f"Successfully loaded retriever for collection: {uid}")
                except Exception as e:
                    # print(f"Error loading collection {uid}: {str(e)}")
                    self.logger.error(f"Error loading collection {uid}: {str(e)}")
                    continue
            
            # Create a multi-vector retriever that combines all retrievers
            if not retrievers:
                raise ValueError("No valid retrievers could be created from the provided unique IDs")
                
            # retrievers = [vs.as_retriever() for vs in retrievers]
            # mretriever = MultiVectorRetriever(retrievers=retrievers)
            # Retrieve relevant documents
            # db_res = mretriever.get_relevant_documents(query)
            # Combine them with EnsembleRetriever
            ensemble_retriever = EnsembleRetriever(
                retrievers=retrievers,
                weights=[1.0/len(retrievers)] * len(retrievers)  # Equal weights
            )

            # Get documents
            db_res = ensemble_retriever.get_relevant_documents(query)
            
            # Format the retrieved documents
            formatted_db_res = "\n".join([doc.page_content for doc in db_res])
            # print(100*"=")
            # print(f"Formatted database results: {formatted_db_res}")
            # print(100*"=")
            self.logger.info(f"============================================================================================================================================")
            self.logger.info(f"Formatted database results: {formatted_db_res}")
            self.logger.info(f"============================================================================================================================================")
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
            -Only answer question based on the database query results and chat history, donot generate any other information.
            -Do not use phrases like: 'here is the reponse:' etc to give your answer.
            -Give one followup question only. not more.
            """    
            self.logger.info(f"going inside llm groq input: {prompt}")
            try:
                stream = self.openai_client.chat.completions.create(
                        messages=[
                            {
                                "role": "system",
                                "content": prompt
                            },
                            {
                                "role": "user",
                                "content": f"The chat history is: {chat_history}. The user query is: {query}"
                            }
                        ],
                        model="llama-3.3-70b-versatile",
                        extra_body={
                        "chat_template_kwargs": {"enable_thinking": False}},

                    )
                full_response = stream.choices[0].message.content
                print(f"Generated response: {full_response}")
            except Exception as e:
                self.logger.info(f"error while generating response: {str(e)}")

            print(f"Reply: {full_response}")
            response_payload = {
                    "query": query,
                    "status": "success",
                    "result": full_response,
                    "metadata": "metadata"
                }
            return response_payload
        except Exception as e:
            self.logger.error(f"Error processing message: {str(e)}")
            # Optionally send an error response
            error_payload = {
                    "query": query,
                    "status": "Failed",
                    "result": "",
                    "metadata": "metadata"
                    }
            return error_payload
        

if __name__ == "__main__":
    vocally_chat_pipeline = VocallyChatPipeline()
    # Example message to process
    example_message = [{'query': "what's the comparison about other years", 'noteRef': [764], 'chat_id': '468', 'uniqueId': ['8f3678cf-864a-477c-a1af-0c299f1a68ad']}]    
    response = vocally_chat_pipeline.process_message(example_message)
    print("Response:", response)