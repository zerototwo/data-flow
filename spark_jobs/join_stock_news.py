import json
from kafka import KafkaConsumer

BROKER = "kafka:9092"
TOPIC = "formatted_stock"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[BROKER],
    value_deserializer=lambda x: json.loads(x.decode()),
    auto_offset_reset='earliest',
    group_id="simple_consumer"
)

print("Start consuming messages...")

for msg in consumer:
    print(f"Received message: {msg.value}")