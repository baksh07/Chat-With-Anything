from types import SimpleNamespace

import pytest

from chat_pipeline import ChatPipeline


@pytest.mark.parametrize(
    "message",
    [
        {},
        {"query": "", "chat_id": "chat-1", "uniqueId": ["doc-1"]},
        {"query": "question", "chat_id": "", "uniqueId": ["doc-1"]},
        {"query": "question", "chat_id": "chat-1", "uniqueId": []},
        [
            {"query": "first", "chat_id": "chat-1", "uniqueId": ["doc-1"]},
            {"query": "second", "chat_id": "chat-1", "uniqueId": ["doc-1"]},
        ],
    ],
)
def test_invalid_messages_are_rejected(message):
    with pytest.raises((TypeError, ValueError)):
        ChatPipeline._normalize_message(message)


def test_process_message_returns_generated_answer():
    calls = {}

    def create_completion(**kwargs):
        calls.update(kwargs)
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content=" Answer text "))]
        )

    pipeline = ChatPipeline.__new__(ChatPipeline)
    pipeline.settings = SimpleNamespace(llm_model="test-model")
    pipeline.logger = SimpleNamespace(info=lambda *_args, **_kwargs: None)
    pipeline.openai_client = SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=create_completion))
    )
    pipeline.extract_chat_history = lambda _chat_id: ["history"]
    pipeline._retrieve_documents = lambda _query, _collection_ids: "document context"

    result = pipeline.process_message(
        {
            "query": "question",
            "chat_id": "chat-1",
            "uniqueId": ["doc-1"],
        }
    )

    assert result == {
        "query": "question",
        "status": "success",
        "result": "Answer text",
        "metadata": {},
    }
    assert calls["model"] == "test-model"
    assert "document context" in calls["messages"][1]["content"]


def test_empty_model_response_is_an_explicit_error():
    pipeline = ChatPipeline.__new__(ChatPipeline)
    pipeline.settings = SimpleNamespace(llm_model="test-model")
    pipeline.logger = SimpleNamespace(info=lambda *_args, **_kwargs: None)
    pipeline.openai_client = SimpleNamespace(
        chat=SimpleNamespace(
            completions=SimpleNamespace(
                create=lambda **_kwargs: SimpleNamespace(
                    choices=[SimpleNamespace(message=SimpleNamespace(content="  "))]
                )
            )
        )
    )
    pipeline.extract_chat_history = lambda _chat_id: []
    pipeline._retrieve_documents = lambda _query, _collection_ids: "document context"

    with pytest.raises(RuntimeError, match="empty response"):
        pipeline.process_message(
            {
                "query": "question",
                "chat_id": "chat-1",
                "uniqueId": ["doc-1"],
            }
        )
