import requests
import json
from kafka import KafkaProducer
import time

API_KEY = "85a882471feb42be92692ca46a2f3a01"
TOPIC = "raw_news"
BROKER = "localhost:9092"

producer = KafkaProducer(bootstrap_servers=[BROKER], value_serializer=lambda x: json.dumps(x).encode())

def fetch_news():
    url = f"https://newsapi.org/v2/top-headlines?country=us&category=business&apiKey={API_KEY}"
    resp = requests.get(url)
    articles = resp.json().get("articles", [])
    for art in articles:
        record = {
            "title": art.get("title"),
            "publishedAt": art.get("publishedAt"),
            "description": art.get("description"),
            "source": art.get("source", {}).get("name"),
            "url": art.get("url")
        }
        producer.send(TOPIC, record)
    producer.flush()

if __name__ == "__main__":
    while True:
        fetch_news()
        print("News data produced.")
        time.sleep(180)  # 每3分钟采一次