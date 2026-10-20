import asyncio
import json
import time
from typing import Any, Dict, List, Optional
from uuid import uuid4

import bentoml
from pydantic import BaseModel, Field
from starlette.responses import StreamingResponse

from chat_pipeline import ChatPipeline
from logger_config import setup_logger


class Message(BaseModel):
    query: str = Field(..., min_length=1)
    noteRef: List[Any] = Field(default_factory=list)
    chat_id: str = Field(..., min_length=1)
    uniqueId: List[str] = Field(..., min_items=1)


class ChatCompletionRequest(BaseModel):
    model: Optional[str] = "chat-with-anything-llm"
    messages: List[Message] = Field(..., min_items=1)
    max_tokens: Optional[int] = 4096
    temperature: Optional[float] = 0.1
    stream: Optional[bool] = False


async def _response_stream(text: str, request_id: str, model: str):
    created = int(time.time())
    words = text.split()
    for index, word in enumerate(words):
        chunk = {
            "id": request_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [
                {
                    "index": 0,
                    "delta": {
                        "content": word + (" " if index < len(words) - 1 else "")
                    },
                    "finish_reason": None,
                }
            ],
        }
        yield f"data: {json.dumps(chunk)}\n\n"
    yield "data: [DONE]\n\n"


@bentoml.service(
    http={
        "cors": {
            "enabled": True,
            "access_control_allow_origins": ["*"],
            "access_control_allow_methods": ["POST", "OPTIONS"],
            "access_control_allow_credentials": False,
            "access_control_allow_headers": ["Content-Type", "Authorization"],
            "access_control_max_age": 1200,
            "access_control_expose_headers": ["Content-Length"],
        }
    },
    traffic={"timeout": 3600, "max_concurrency": 256},
    workers=2,
)
class ChatService:
    def __init__(self) -> None:
        self.pipeline = ChatPipeline()
        self.logger = setup_logger("ChatService", "chat_service.log")
        self.logger.info("Chat service initialized.")

    @bentoml.api(route="/chat/completions")
    async def chat_completions(
        self,
        messages: List[Message],
        model: str = "chat-with-anything-llm",
        stream: bool = False,
    ) -> Any:
        request_id = f"chat_{uuid4().hex}"
        self.logger.info(
            "Chat request %s started with %d message(s).",
            request_id,
            len(messages),
        )
        if not messages:
            raise ValueError("At least one message is required.")
        if len(messages) != 1:
            raise ValueError("Exactly one message is supported per chat request.")

        pipeline_messages: List[Dict[str, Any]] = [
            {
                "query": message.query,
                "noteRef": message.noteRef,
                "chat_id": message.chat_id,
                "uniqueId": message.uniqueId,
            }
            for message in messages
        ]
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            None,
            self.pipeline.process_message,
            pipeline_messages,
        )
        response_content = result["result"]
        self.logger.info("Chat request %s completed.", request_id)

        if stream:
            return StreamingResponse(
                _response_stream(response_content, request_id, model),
                media_type="text/event-stream",
            )

        return {
            "id": request_id,
            "object": "chat.completion",
            "created": int(time.time()),
            "model": model,
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": response_content},
                    "finish_reason": "stop",
                }
            ],
        }
