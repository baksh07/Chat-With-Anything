from kafka import KafkaProducer
import json

# Kafka configuration
KAFKA_BROKER = '216.48.183.140:9092'
TOPIC_NAME = 'vocally_input'

# # Audio URL to send
# audio_url = "https://lingoscaledata.s3.us-east-2.amazonaws.com/test/1ff1b50b-3736-4072-ac1e-7fd109722171.mp3?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=AKIA6ODU3R7HODSP7XRK%2F20250507%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20250507T071235Z&X-Amz-Expires=82400&X-Amz-Signature=3e0fcd432b20c987d039d2394fe5f395dab0258ca48dd9151c31bfe1d65d5c4f&X-Amz-SignedHeaders=host&x-amz-checksum-mode=ENABLED&x-id=GetObject"

# Construct the Kafka message payload
payload = {
  "metadata": {
    "uniqueId": "be9738a8-3a9b-49a5-8e5e-09edd83e0119",
    "inputType": "file-upload",
    "file_type": "pdf"
  },
  "audio_url": "https://lingoscaledata.s3.us-east-2.amazonaws.com/test/be9738a8-3a9b-49a5-8e5e-09edd83e0119.mp3?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=AKIA6ODU3R7HODSP7XRK%2F20250522%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20250522T064459Z&X-Amz-Expires=82400&X-Amz-Signature=5dcb27b6deb88a2f80c8700656ad6a3963db517960151f4eb530c3aaba736f03&X-Amz-SignedHeaders=host&x-amz-checksum-mode=ENABLED&x-id=GetObject"
}
# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send the message
producer.send(TOPIC_NAME, value=payload)
producer.flush()
print(f"Sent test message to topic '{TOPIC_NAME}'")

# Close the producer
producer.close()
