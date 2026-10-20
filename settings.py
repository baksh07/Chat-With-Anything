import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent


def _configured_path(name: str, default: Path) -> Path:
    path = Path(os.getenv(name, str(default))).expanduser()
    return path if path.is_absolute() else PROJECT_ROOT / path


@dataclass(frozen=True)
class Settings:
    database_host: str
    database_port: int
    database_name: str
    database_user: str
    database_password: str
    database_schema: str
    llm_api_key: str
    llm_base_url: str
    llm_model: str
    summary_llm_api_key: str
    summary_llm_base_url: str
    summary_llm_model: str
    groq_api_key: str
    kafka_bootstrap_servers: List[str]
    kafka_input_topic: str
    kafka_output_topic: str
    kafka_query_input_topic: str
    kafka_query_output_topic: str
    processor_audio_url: str
    processor_text_url: str
    chroma_persist_dir: Path
    upload_dir: Path
    scraped_json_dir: Path

    @classmethod
    def from_environment(cls) -> "Settings":
        load_dotenv(PROJECT_ROOT / ".env")

        database_password = os.getenv("DB_PASSWORD", "")
        bootstrap_servers = [
            server.strip()
            for server in os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", "216.48.183.140:9092"
            ).split(",")
            if server.strip()
        ]
        if not bootstrap_servers:
            raise RuntimeError(
                "KAFKA_BOOTSTRAP_SERVERS must contain at least one broker."
            )

        return cls(
            database_host=os.getenv("DB_HOST", "91.203.132.196"),
            database_port=int(os.getenv("DB_PORT", "5432")),
            database_name=os.getenv("DB_NAME", "Unifile_Chat"),
            database_user=os.getenv("DB_USER", "slaif"),
            database_password=database_password,
            database_schema=os.getenv("DB_SCHEMA", "sl_schema"),
            llm_api_key=os.getenv("CHAT_LLM_API_KEY", "llm"),
            llm_base_url=os.getenv(
                "CHAT_LLM_BASE_URL", "http://216.48.182.241:3535/v1"
            ),
            llm_model=os.getenv("CHAT_LLM_MODEL", "llama-3.3-70b-versatile"),
            summary_llm_api_key=os.getenv("SUMMARY_LLM_API_KEY", "local-llm"),
            summary_llm_base_url=os.getenv(
                "SUMMARY_LLM_BASE_URL", "http://164.52.196.111:3005"
            ),
            summary_llm_model=os.getenv("SUMMARY_LLM_MODEL", "sl-llm"),
            groq_api_key=os.getenv("GROQ_API_KEY", "").strip(),
            kafka_bootstrap_servers=bootstrap_servers,
            kafka_input_topic=os.getenv("KAFKA_INPUT_TOPIC", "chat-input"),
            kafka_output_topic=os.getenv("KAFKA_OUTPUT_TOPIC", "chat_ai_output"),
            kafka_query_input_topic=os.getenv(
                "KAFKA_QUERY_INPUT_TOPIC", "chat-input-query"
            ),
            kafka_query_output_topic=os.getenv(
                "KAFKA_QUERY_OUTPUT_TOPIC", "chat_ai_output_query"
            ),
            processor_audio_url=os.getenv(
                "PROCESSOR_AUDIO_URL",
                "http://216.48.184.150:5999/LingoVoice/process_audio_url/",
            ),
            processor_text_url=os.getenv(
                "PROCESSOR_TEXT_URL",
                "http://216.48.184.150:3999/LingoVoice/process_text/",
            ),
            chroma_persist_dir=_configured_path(
                "CHROMA_PERSIST_DIR", PROJECT_ROOT / "chroma_db"
            ),
            upload_dir=_configured_path("UPLOAD_DIR", PROJECT_ROOT / "UPLOAD"),
            scraped_json_dir=_configured_path(
                "SCRAPED_JSON_DIR", PROJECT_ROOT / "myproject"
            ),
        )

    def database_connection_kwargs(self) -> Dict[str, object]:
        if not self.database_password:
            raise RuntimeError(
                "DB_PASSWORD is required. Add it to the local .env file "
                "or process environment."
            )
        return {
            "host": self.database_host,
            "port": self.database_port,
            "dbname": self.database_name,
            "user": self.database_user,
            "password": self.database_password,
        }
