from types import SimpleNamespace

import pytest
import requests

from SERVICES.web_scrape_service import WebScraper


def test_scraper_extracts_text_and_title(monkeypatch):
    response = SimpleNamespace(
        encoding="utf-8",
        text="<html><title>Example page</title><body>Useful page content</body></html>",
        raise_for_status=lambda: None,
    )
    monkeypatch.setattr(requests, "get", lambda *_args, **_kwargs: response)

    result = WebScraper().scrape(
        "https://example.com",
        min_line_length=1,
    )

    assert result == ("Example page\nUseful page content", "Example page")


def test_scraper_rejects_invalid_urls():
    with pytest.raises(ValueError, match="Invalid URL"):
        WebScraper().scrape("file:///etc/passwd")


def test_scraper_propagates_request_failures(monkeypatch):
    def fail_request(*_args, **_kwargs):
        raise requests.Timeout("request timed out")

    monkeypatch.setattr(requests, "get", fail_request)

    with pytest.raises(requests.Timeout):
        WebScraper().scrape("https://example.com")
