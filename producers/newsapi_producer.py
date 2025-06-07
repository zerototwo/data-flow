import requests
import json
from kafka import KafkaProducer

API_KEY = "45a41e33d32d4a4d91e4182e45e437f7"
TOPIC = "raw_news"
BROKER = "kafka:9092"

producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=lambda x: json.dumps(x).encode()
)

def fetch_news():
    url = f"https://newsapi.org/v2/top-headlines?category=business&language=en&apiKey={API_KEY}"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        articles = data.get("articles", [])
    except Exception as e:
        print(f"Failed to fetch news data: {e}")
        return

    count = 0
    for article in articles:
        try:
            record = {
                "title": article["title"],
                "publishedAt": article["publishedAt"],
                "source": article["source"]["name"],
                "description": article["description"]
            }
            producer.send(TOPIC, record)
            count += 1
        except Exception as e:
            print(f"Failed to send record to Kafka: {e}")
    producer.flush()
    print(f"Produced {count} news records to topic {TOPIC}.")

if __name__ == "__main__":
    fetch_news()