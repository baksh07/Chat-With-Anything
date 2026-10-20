import pytest

import settings


def test_settings_load_environment(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "load_dotenv", lambda *_args, **_kwargs: None)
    monkeypatch.setenv("DB_PASSWORD", "test-password")
    monkeypatch.setenv("DB_NAME", "test-database")
    monkeypatch.setenv("DB_PORT", "6543")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker-a:9092, broker-b:9092")
    monkeypatch.setenv("UPLOAD_DIR", str(tmp_path / "uploads"))

    configured = settings.Settings.from_environment()

    assert configured.database_name == "test-database"
    assert configured.database_port == 6543
    assert configured.kafka_bootstrap_servers == ["broker-a:9092", "broker-b:9092"]
    assert configured.upload_dir == tmp_path / "uploads"
    assert configured.database_connection_kwargs()["password"] == "test-password"


def test_database_password_is_required_when_connecting(monkeypatch):
    monkeypatch.setattr(settings, "load_dotenv", lambda *_args, **_kwargs: None)
    monkeypatch.delenv("DB_PASSWORD", raising=False)

    configured = settings.Settings.from_environment()

    with pytest.raises(RuntimeError, match="DB_PASSWORD is required"):
        configured.database_connection_kwargs()


def test_empty_kafka_broker_configuration_is_rejected(monkeypatch):
    monkeypatch.setattr(settings, "load_dotenv", lambda *_args, **_kwargs: None)
    monkeypatch.setenv("DB_PASSWORD", "test-password")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", " , ")

    with pytest.raises(RuntimeError, match="KAFKA_BOOTSTRAP_SERVERS"):
        settings.Settings.from_environment()
