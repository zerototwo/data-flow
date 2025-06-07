from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json

JOINED_TOPIC = "joined_data"
BROKER = "kafka:9092"
ES_HOST = "http://elasticsearch:9200"
INDEX_NAME = "stock_news"

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

    for message in consumer:
        record = message.value
        res = es.index(index=INDEX_NAME, document=record)
        print(f"Inserted to ES: {res['_id']}")

# 注意：这里不要直接执行 run()，让 Airflow 调用它！
if __name__ == "__main__":
    run()