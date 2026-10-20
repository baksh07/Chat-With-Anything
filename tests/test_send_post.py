from types import SimpleNamespace

import pytest

import send_post
from send_post import LingoVoiceProcessor


def test_audio_processor_uses_configured_url_and_timeout(monkeypatch):
    calls = {}

    def fake_post(url, **kwargs):
        calls.update(url=url, **kwargs)
        return SimpleNamespace(
            status_code=200,
            raise_for_status=lambda: None,
            json=lambda: {"result": "transcription"},
        )

    monkeypatch.setattr(send_post.requests, "post", fake_post)
    processor = LingoVoiceProcessor(
        "https://processor.example/audio",
        "https://processor.example/text",
        timeout=12,
    )

    result = processor.process_audio_url("https://files.example/audio.wav", False)

    assert calls["url"] == "https://processor.example/audio"
    assert calls["timeout"] == 12
    assert calls["params"]["summarization"] == "false"
    assert result == {"status_code": 200, "response": {"result": "transcription"}}


def test_audio_processor_propagates_http_errors(monkeypatch):
    def fake_post(_url, **_kwargs):
        def raise_for_status():
            raise RuntimeError("HTTP failure")

        return SimpleNamespace(raise_for_status=raise_for_status)

    monkeypatch.setattr(send_post.requests, "post", fake_post)
    processor = LingoVoiceProcessor(
        "https://processor.example/audio",
        "https://processor.example/text",
    )

    with pytest.raises(RuntimeError, match="HTTP failure"):
        processor.process_audio_url("https://files.example/audio.wav")
