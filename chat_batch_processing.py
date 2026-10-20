from kafka import KafkaConsumer, KafkaProducer
from send_post import LingoVoiceProcessor
import json
import logging
import signal
import sys
from datetime import datetime
import requests
from data_ingestion_pipeline import LoadData
from settings import Settings

settings = Settings.from_environment()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KafkaLingoBridge:
    def __init__(self):
        self.running = True
        self.kafka_config = {
            'bootstrap_servers': settings.kafka_bootstrap_servers,
            'input_topic': settings.kafka_input_topic,
            'output_topic': settings.kafka_output_topic
        }
        self.load_data = LoadData()
        self.consumer = None
        self.producer = None
        self.upload_dir = str(settings.upload_dir)
        self.processor = LingoVoiceProcessor(
            settings.processor_audio_url,
            settings.processor_text_url,
        )

    def initialize(self):
        try:
            self.consumer = KafkaConsumer(
                self.kafka_config['input_topic'],
                bootstrap_servers=self.kafka_config['bootstrap_servers'],
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='chat-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )

            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_config['bootstrap_servers'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )

            logger.info("Kafka consumer and producer initialized successfully.")
        except Exception as e:
            logger.error(f"Initialization failed: {str(e)}")
            raise

    def shutdown(self):
        logger.info("Shutting down Kafka service...")
        self.running = False
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.flush()
            self.producer.close()
        logger.info("Shutdown complete.")

    def process_message(self, message):
        try:
            audio_url = message.get('audio_url')
            metadata = message.get('metadata')
            unique_id = metadata["uniqueId"]
            logger.info(f"Processing message with unique ID: {unique_id}")
            if metadata["file_type"] == "pdf":
                pass
                # date = datetime.today().strftime("%Y-%m-%d")
                # filename = unique_id + "_" + date + ".pdf"
                # file_path = self.upload_dir + "/" + filename
                # logger.info(f"Processing PDF file: {file_path}")

                # response = requests.get(audio_url)
                # with open(file_path, 'wb') as f:
                #     f.write(response.content)
                # logger.info(f"Saved PDF file to: {file_path}")
                # # Process the PDF file

                # text = self.load_data.load_pdf(file_path)



            elif metadata["file_type"] == "text":
                pass


            elif metadata["file_type"] == "docx":
                pass

            elif metadata["file_type"] == "doc":
                pass

            elif metadata["file_type"] == "pptx":
                pass

            elif metadata["file_type"] == "ppt":
                pass



            elif metadata["file_type"] == "audio":
                if not audio_url:
                    raise ValueError("Missing 'audio_url' in the message.")

                logger.info(f"Processing audio URL: {audio_url}")
                result = self.processor.process_audio_url(audio_url)

                response_payload = {
                    "input_audio_url": audio_url,
                    "result": result,
                    "metadata": metadata
                }

                self.producer.send(self.kafka_config['output_topic'], response_payload)
                logger.info(f"Published result to {self.kafka_config['output_topic']}")
            else:
                logger.warning(f"Unsupported message type: {metadata['type']}")

        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")

    def run(self):
        try:
            self.initialize()
            logger.info("Listening for messages...")

            while self.running:
                message_batch = self.consumer.poll(timeout_ms=1000)
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        if not self.running:
                            break
                        self.process_message(message.value)

        except Exception as e:
            logger.error(f"Fatal error: {str(e)}")
        finally:
            self.shutdown()


def setup_signal_handlers(service):
    def handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        service.shutdown()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


if __name__ == "__main__":
    bridge = KafkaLingoBridge()
    setup_signal_handlers(bridge)
    bridge.run()
