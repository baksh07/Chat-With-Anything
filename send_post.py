from typing import Any, Dict, Optional

import requests

from settings import Settings


class LingoVoiceProcessor:
    def __init__(
        self,
        base_url: str,
        text_url: Optional[str] = None,
        timeout: int = 60,
    ) -> None:
        self.base_url = base_url
        self.text_url = text_url or Settings.from_environment().processor_text_url
        self.timeout = timeout
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def process_text(self, text: str, summarization: bool = True) -> Dict[str, Any]:
        response = requests.post(
            self.text_url,
            headers=self.headers,
            json={"Text": text},
            params={"summarization": str(summarization).lower()},
            timeout=self.timeout,
        )
        response.raise_for_status()
        return {"status_code": response.status_code, "response": response.json()}

    def process_audio_url(
        self,
        audio_url: str,
        summarization: bool = True,
    ) -> Dict[str, Any]:
        response = requests.post(
            self.base_url,
            headers=self.headers,
            json={"audiourl": audio_url},
            params={
                "summarization": str(summarization).lower(),
                "title": "true",
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        return {"status_code": response.status_code, "response": response.json()}


if __name__ == "__main__":
    settings = Settings.from_environment()
    processor = LingoVoiceProcessor(
        settings.processor_audio_url,
        settings.processor_text_url,
    )
    audio_url = input("Audio URL: ").strip()
    if not audio_url:
        raise ValueError("An audio URL is required.")
    print(processor.process_audio_url(audio_url))
