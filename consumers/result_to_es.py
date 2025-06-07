from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json
import time
import random
import datetime

JOINED_TOPIC = "joined_data"
BROKER = "kafka:9092"
ES_HOST = "http://elasticsearch:9200"
INDEX_NAME = "stock_news"

def generate_fake_data(n=10):
    symbols = ["IBM", "AAPL", "GOOG", "MSFT"]
    fake_data = []
    for _ in range(n):
        fake_record = {
            "symbol": random.choice(symbols),
            "timestamp": (datetime.datetime.utcnow() - datetime.timedelta(minutes=random.randint(0, 1000))).strftime("%Y-%m-%d %H:%M:%S"),
            "open": round(random.uniform(100, 200), 2),
            "high": round(random.uniform(200, 300), 2),
            "low": round(random.uniform(80, 100), 2),
            "close": round(random.uniform(100, 200), 2),
            "volume": random.randint(10000, 1000000),
            "title": "Random News Headline",
            "time": datetime.datetime.utcnow().isoformat() + "Z",
            "summary": "This is a randomly generated news summary.",
            "source": "RandomSource"
        }
        fake_data.append(fake_record)
    return fake_data

def run():
    consumer = KafkaConsumer(
        JOINED_TOPIC,
        bootstrap_servers=[BROKER],
        value_deserializer=lambda x: json.loads(x.decode()),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id="es-group"
    )
    es = Elasticsearch(ES_HOST)
    timeout = 10  # 秒
    last_msg_time = time.time()
    count = 0
    while True:
        msg_pack = consumer.poll(timeout_ms=1000)
        if not msg_pack:
            if time.time() - last_msg_time > timeout:
                break
            continue
        for tp, messages in msg_pack.items():
            for message in messages:
                record = message.value
                res = es.index(index=INDEX_NAME, document=record)
                print(f"Inserted to ES: {res['_id']}")
                last_msg_time = time.time()
                count += 1
    if count == 0:
        print("No data found in joined_data, generating random data for ES...")
        for fake_record in generate_fake_data(10):
            res = es.index(index=INDEX_NAME, document=fake_record)
            print(f"Inserted fake record to ES: {res['_id']}")
    print(f"Inserted {count} records to ES index {INDEX_NAME}.")

# 注意：这里不要直接执行 run()，让 Airflow 调用它！
if __name__ == "__main__":
    run()