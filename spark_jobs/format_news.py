import json
import time
from kafka import KafkaConsumer, KafkaProducer

RAW_TOPIC = "raw_news"
FORMATTED_TOPIC = "formatted_news"
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
    timeout = 10  # 秒
    last_msg_time = time.time()
    while True:
        msg_pack = consumer.poll(timeout_ms=1000)
        if not msg_pack:
            if time.time() - last_msg_time > timeout:
                break
            continue
        for tp, messages in msg_pack.items():
            for message in messages:
                raw = message.value
                formatted = {
                    "title": raw["title"],
                    "time": raw["publishedAt"],
                    "summary": raw["description"],
                    "source": raw["source"]
                }
                producer.send(FORMATTED_TOPIC, formatted)
                count += 1
                last_msg_time = time.time()
    producer.flush()
    print(f"Formatted and produced {count} news records to topic {FORMATTED_TOPIC}.")

if __name__ == "__main__":
    run()