import pytest

from data_ingestion_pipeline import DataProcessor, LoadData


def test_load_text_reads_utf8(tmp_path):
    source = tmp_path / "note.txt"
    source.write_text("Hello, world!", encoding="utf-8")

    assert LoadData().load_text(source) == "Hello, world!"


def test_process_file_rejects_unsupported_extensions(tmp_path):
    source = tmp_path / "archive.bin"
    source.write_bytes(b"data")

    with pytest.raises(ValueError, match="Unsupported file format"):
        DataProcessor().process_file(source, save_to_txt=False)


def test_url_loader_rejects_non_http_schemes():
    with pytest.raises(ValueError, match="HTTP and HTTPS"):
        LoadData().load_url("file:///etc/passwd")


def test_audio_loader_reports_unimplemented_transcription():
    with pytest.raises(NotImplementedError, match="audio processor service"):
        LoadData().load_audio("recording.mp3")


def test_url_file_is_loaded_before_fetching(tmp_path, monkeypatch):
    source = tmp_path / "source.url"
    source.write_text("https://example.com/article\n", encoding="utf-8")
    loader = LoadData()
    monkeypatch.setattr(loader, "load_url", lambda url: f"fetched:{url}")

    assert loader.load_url_file(source) == "fetched:https://example.com/article"


def test_batch_process_requires_existing_directory(tmp_path):
    missing = tmp_path / "missing"

    with pytest.raises(NotADirectoryError, match="does not exist"):
        DataProcessor().batch_process(missing)
