import bentoml
from vocally_chat_pipeline import VocallyChatPipeline
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
from starlette.responses import StreamingResponse
import asyncio
import time
import json
import logging
import os
from logger_config import setup_logger

class Message(BaseModel):
    query: str
    noteRef: List
    chat_id: str
    uniqueId: List

class ChatCompletionRequest(BaseModel):
    model: Optional[str] = "sl-llm"
    messages: List[Message]
    max_tokens: Optional[int] = 4096
    temperature: Optional[float] = 0.1
    stream: Optional[bool] = False

async def _resp_async_generator(text_resp: str):
    # simulate streaming by sending each token with delay
    tokens = text_resp.split(" ")
    for i, token in enumerate(tokens):
        chunk = {
            "id": i,
            "object": "chat.completion.chunk",
            "created": time.time(),
            "model": "sl-llm",
            "choices": [{"delta": {"content": token + " "}}],
        }
        yield f"data: {json.dumps(chunk)}\n\n"
        await asyncio.sleep(0.2)
    yield "data: [DONE]\n\n"

@bentoml.service(
    http={
        "cors": {
            "enabled": True,
            "access_control_allow_origins": "*",
            "access_control_allow_methods": ["GET", "OPTIONS", "POST", "HEAD", "PUT"],
            "access_control_allow_credentials": True,
            "access_control_allow_headers": ["*"],
            "access_control_allow_origin_regex": "https://.*\.my_org\.com",
            "access_control_max_age": 1200,
            "access_control_expose_headers": ["Content-Length"]
        }
    },
    traffic={
        "timeout": 3600,
        "max_concurrency": 256,
    },
    workers=2
)
class VocallyChatService:
    def __init__(self):
        self.pipeline = VocallyChatPipeline()
        # Set up centralized logger
        self.logger = setup_logger("VocallyChatService")

        self.logger.info("VocallyChatService initialized.")

    @bentoml.api(route="/chat/completions")
    async def chat_completions(self, messages: List[Message], model: str, stream: bool = False):
        request_id = f"chat_{int(time.time())}"
        self.logger.info(f"CHAT REQUEST {request_id} | STARTED")
        self.logger.info(f"Input Messages: {messages}")
        if messages:
            input_prompt = []

            for data in messages:
                intermediate = {
                    "query": data.query,
                    "noteRef": data.noteRef,
                    "chat_id": data.chat_id,
                    "uniqueId": data.uniqueId,
                }
                input_prompt.append(intermediate)
            # self.logger.info(f"Input Prompt: {input_prompt}")
            self.logger.info(f"going inside pipeline process_message with input prompt: {input_prompt}")
            output = self.pipeline.process_message(input_prompt)
            self.logger.info(f"Output from pipeline: {output}")

            self.logger.info(f"CHAT REQUEST {request_id} | COMPLETED")
            resp_content = output['result']
            self.logger.info(f"CHAT REQUEST {request_id} | RESPONSE: {resp_content}")
        else:
            self.logger.warning(f"CHAT REQUEST {request_id} | INVALID DATA")
            resp_content = "Please pass valid data! check prompt"

        if stream:
            return StreamingResponse(
                _resp_async_generator(resp_content),
                media_type="application/x-ndjson"
            )

        return {
            "id": request_id,
            "object": "chat.completion",
            "created": time.time(),
            "model": "sl-llm",
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": resp_content
                }
            }],
        }
