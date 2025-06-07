from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json

KAFKA_TOPIC = "result"  # 或 "formatted_stock" / "formatted_news"
KAFKA_BROKER = "localhost:9092"

ES_HOST = "localhost"
ES_PORT = 9200
ES_INDEX = "stock-news"

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True
)

es = Elasticsearch([{'host': ES_HOST, 'port': ES_PORT, 'scheme': "http"}])

for msg in consumer:
    doc = msg.value
    # 可自定义处理/去重/数据清洗
    res = es.index(index=ES_INDEX, document=doc)
    print(f"Wrote to ES: {res['result']} - {doc}")