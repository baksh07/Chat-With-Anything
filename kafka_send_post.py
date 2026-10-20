import argparse
import json
from uuid import uuid4

from kafka import KafkaProducer

from settings import Settings


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Publish a sample file-processing message to Kafka."
    )
    parser.add_argument("audio_url", help="Public or signed URL for the source file.")
    parser.add_argument(
        "--file-type",
        default="mp3",
        choices=("pdf", "ppt", "pptx", "doc", "docx", "txt", "mp3", "wav", "mp4"),
    )
    args = parser.parse_args()

    settings = Settings.from_environment()
    unique_id = str(uuid4())
    payload = {
        "metadata": {
            "uniqueId": unique_id,
            "inputType": "file-upload",
            "file_type": args.file_type,
        },
        "audio_url": args.audio_url,
    }

    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
    )
    try:
        metadata = producer.send(
            settings.kafka_input_topic,
            value=payload,
        ).get(timeout=10)
        print(
            "Published message "
            f"{unique_id} to {metadata.topic} partition {metadata.partition}."
        )
    finally:
        producer.close()


if __name__ == "__main__":
    main()
