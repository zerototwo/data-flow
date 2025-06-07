import json
from kafka import KafkaConsumer, KafkaProducer

RAW_TOPIC = "raw_news"
CLEAN_TOPIC = "clean_news"
BROKER = "kafka:9092"

consumer = KafkaConsumer(
    RAW_TOPIC,
    bootstrap_servers=[BROKER],
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode())
)

producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=lambda x: json.dumps(x).encode()
)

def run():
    count = 0
    for message in consumer:
        try:
            raw = message.value
            clean = {
                "title": raw["title"],
                "time": raw["publishedAt"],
                "summary": raw["description"],
                "source": raw["source"]
            }
            producer.send(CLEAN_TOPIC, clean)
            count += 1
        except Exception as e:
            print(f"Failed to format or send message: {e}")
    producer.flush()
    print(f"Formatted and produced {count} news records to topic {CLEAN_TOPIC}.")

if __name__ == "__main__":
    run()