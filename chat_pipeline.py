import logging
from typing import Any, Dict, List, Mapping, Optional, Sequence, Union

from logger_config import setup_logger
from settings import Settings

ChatMessage = Mapping[str, Any]
MessageInput = Union[ChatMessage, Sequence[ChatMessage]]


class ChatPipeline:
    def __init__(
        self,
        settings: Optional[Settings] = None,
        openai_client: Optional[Any] = None,
        db_pool: Optional[Any] = None,
        embeddings: Optional[Any] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.settings = settings or Settings.from_environment()
        self.logger = logger or setup_logger("ChatPipeline", "chat_pipeline.log")
        self.openai_client = openai_client
        if self.openai_client is None:
            from openai import OpenAI

            self.openai_client = OpenAI(
                api_key=self.settings.llm_api_key,
                base_url=self.settings.llm_base_url,
            )
        self.db_pool = db_pool
        if self.db_pool is None:
            from psycopg2.pool import ThreadedConnectionPool

            self.db_pool = ThreadedConnectionPool(
                1,
                10,
                **self.settings.database_connection_kwargs(),
            )
        self.chroma_persist_dir = str(self.settings.chroma_persist_dir)

        if embeddings is None:
            from langchain.embeddings import HuggingFaceEmbeddings

            embeddings = HuggingFaceEmbeddings(
                model_name="sentence-transformers/all-MiniLM-L6-v2",
                model_kwargs={"device": "cpu"},
            )
        self.embeddings = embeddings

        self.logger.info("Chat pipeline initialized.")

    def close(self) -> None:
        self.db_pool.closeall()

    def extract_chat_history(self, chat_id: Union[str, Sequence[str]]) -> List[Any]:
        chat_ids = [chat_id] if isinstance(chat_id, str) else list(chat_id)
        if not chat_ids:
            return []

        from psycopg2 import sql

        connection = self.db_pool.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql.SQL("SELECT * FROM {}.message WHERE chat_id = ANY(%s)").format(
                        sql.Identifier(self.settings.database_schema)
                    ),
                    (chat_ids,),
                )
                return cursor.fetchall()[-10:]
        except Exception:
            connection.rollback()
            self.logger.exception("Could not retrieve chat history.")
            raise
        finally:
            self.db_pool.putconn(connection)

    @staticmethod
    def _normalize_message(message: MessageInput) -> ChatMessage:
        if isinstance(message, Mapping):
            normalized = message
        elif isinstance(message, Sequence) and not isinstance(message, (str, bytes)):
            if len(message) != 1 or not isinstance(message[0], Mapping):
                raise ValueError("Expected exactly one chat message.")
            normalized = message[0]
        else:
            raise TypeError(
                "Message must be an object or a one-item sequence of objects."
            )

        query = normalized.get("query")
        chat_id = normalized.get("chat_id")
        collection_ids = normalized.get("uniqueId")
        if not isinstance(query, str) or not query.strip():
            raise ValueError("The query must be a non-empty string.")
        if not isinstance(chat_id, (str, list)) or not chat_id:
            raise ValueError("chat_id must be a non-empty string or list of strings.")
        if isinstance(chat_id, list) and any(
            not isinstance(value, str) or not value.strip() for value in chat_id
        ):
            raise ValueError("Each chat_id value must be a non-empty string.")
        if not isinstance(collection_ids, list) or not collection_ids:
            raise ValueError(
                "uniqueId must contain at least one collection identifier."
            )
        if any(
            not isinstance(value, str) or not value.strip() for value in collection_ids
        ):
            raise ValueError("Each collection identifier must be a non-empty string.")

        return normalized

    def _retrieve_documents(self, query: str, collection_ids: List[str]) -> str:
        from langchain.retrievers import EnsembleRetriever
        from langchain.vectorstores import Chroma

        retrievers = []
        for collection_id in dict.fromkeys(collection_ids):
            vectorstore = Chroma(
                collection_name=collection_id,
                persist_directory=self.chroma_persist_dir,
                embedding_function=self.embeddings,
            )
            retrievers.append(vectorstore.as_retriever())

        if not retrievers:
            raise ValueError("No document collections were provided.")

        ensemble = EnsembleRetriever(
            retrievers=retrievers,
            weights=[1.0 / len(retrievers)] * len(retrievers),
        )
        documents = ensemble.get_relevant_documents(query)
        return "\n\n".join(
            document.page_content
            for document in documents
            if getattr(document, "page_content", "").strip()
        )

    def process_message(self, message: MessageInput) -> Dict[str, Any]:
        normalized = self._normalize_message(message)
        query = normalized["query"].strip()
        collection_ids = normalized["uniqueId"]

        self.logger.info(
            "Processing chat request for %d document collection(s).",
            len(collection_ids),
        )
        chat_history = self.extract_chat_history(normalized["chat_id"])
        document_context = self._retrieve_documents(query, collection_ids)
        if not document_context:
            raise ValueError("No relevant document content was found for this query.")

        system_prompt = (
            "Answer the user's question using only the supplied document context and "
            "chat history. If the information is not present, say so clearly. "
            "Be concise, professional, and warm. Do not invent facts. "
            "End with at most one relevant follow-up question."
        )
        completion = self.openai_client.chat.completions.create(
            messages=[
                {"role": "system", "content": system_prompt},
                {
                    "role": "user",
                    "content": (
                        f"Document context:\n{document_context}\n\n"
                        f"Chat history:\n{chat_history}\n\n"
                        f"Question:\n{query}"
                    ),
                },
            ],
            model=self.settings.llm_model,
            extra_body={"chat_template_kwargs": {"enable_thinking": False}},
        )
        answer = completion.choices[0].message.content
        if not isinstance(answer, str) or not answer.strip():
            raise RuntimeError("The language model returned an empty response.")

        return {
            "query": query,
            "status": "success",
            "result": answer.strip(),
            "metadata": {},
        }


if __name__ == "__main__":
    pipeline = ChatPipeline()
    try:
        print(
            pipeline.process_message(
                {
                    "query": "What is the main topic?",
                    "noteRef": [],
                    "chat_id": "example",
                    "uniqueId": ["example-collection"],
                }
            )
        )
    finally:
        pipeline.close()
