import requests
import json
from kafka import KafkaProducer

# Alpha Vantage API 配置
API_KEY = "MAACGA3YB1I09GJ8"  # 可替换为你自己的 Key
SYMBOL = "IBM"
TOPIC = "raw_stock"
BROKER = "kafka:9092"

# Kafka 生产者配置
producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=lambda x: json.dumps(x).encode()
)

# 主函数：从 Alpha Vantage 拉取股票数据并写入 Kafka
def fetch_stock():
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={SYMBOL}&interval=1min&apikey={API_KEY}"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        time_series = data.get("Time Series (1min)", {})
    except Exception as e:
        print(f"Failed to fetch stock data: {e}")
        return

    count = 0
    for dt, info in time_series.items():
        try:
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
            count += 1
        except Exception as e:
            print(f"Failed to send record to Kafka: {e}")
    producer.flush()
    print(f"Produced {count} stock records to topic {TOPIC}.")

# 若直接运行脚本，则执行抓取任务
if __name__ == "__main__":
    fetch_stock()