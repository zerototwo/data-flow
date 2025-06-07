import requests
import json
from kafka import KafkaProducer
import time

API_KEY = "MAACGA3YB1I09GJ8"
SYMBOL = "IBM"
TOPIC = "raw_stock"
BROKER = "localhost:9092"

producer = KafkaProducer(bootstrap_servers=[BROKER], value_serializer=lambda x: json.dumps(x).encode())

def fetch_stock():
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={SYMBOL}&interval=1min&apikey={API_KEY}"
    resp = requests.get(url)
    data = resp.json()
    time_series = data.get("Time Series (1min)", {})
    for dt, info in time_series.items():
        record = {
            "symbol": SYMBOL,
            "timestamp": dt,
            "open": float(info["1. open"]),
            "high": float(info["2. high"]),
            "low": float(info["3. low"]),
            "close": float(info["4. close"]),
            "volume": int(info["5. volume"])
        }
        producer.send(TOPIC, record)
    producer.flush()

if __name__ == "__main__":
    while True:
        fetch_stock()
        print("Stock data produced.")
        time.sleep(60)  # 每分钟采集一次