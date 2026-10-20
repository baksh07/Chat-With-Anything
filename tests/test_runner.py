import json
from types import SimpleNamespace

import pytest

import myproject.runner as runner


def test_scraper_reads_json_output(monkeypatch, tmp_path):
    monkeypatch.setattr(runner, "SCRAPY_PROJECT_DIR", tmp_path)

    def fake_run(command, **_kwargs):
        output_name = command[command.index("-O") + 1]
        (tmp_path / output_name).write_text(
            json.dumps([{"text": "scraped text"}]),
            encoding="utf-8",
        )
        return SimpleNamespace(returncode=0, stderr="")

    monkeypatch.setattr(runner.subprocess, "run", fake_run)

    result = runner.scrape_with_cli("https://example.com", id="document-1")

    assert result == [{"text": "scraped text"}]


def test_scraper_rejects_unsafe_output_ids():
    with pytest.raises(ValueError, match="unsupported characters"):
        runner.scrape_with_cli("https://example.com", id="../outside")


def test_scraper_surfaces_subprocess_failures(monkeypatch):
    monkeypatch.setattr(
        runner.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=1, stderr="crawl failed"),
    )

    with pytest.raises(RuntimeError, match="crawl failed"):
        runner.scrape_with_cli("https://example.com", id="document-1")
