import json
import time
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer

STOCK_TOPIC = "formatted_stock"
NEWS_TOPIC = "formatted_news"
JOINED_TOPIC = "joined_data"
BROKER = "kafka:9092"

def run():
    consumer_stock = KafkaConsumer(
        STOCK_TOPIC,
        bootstrap_servers=[BROKER],
        value_deserializer=lambda x: json.loads(x.decode()),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id="joiner-stock"
    )
    consumer_news = KafkaConsumer(
        NEWS_TOPIC,
        bootstrap_servers=[BROKER],
        value_deserializer=lambda x: json.loads(x.decode()),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id="joiner-news"
    )
    producer = KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=lambda x: json.dumps(x).encode()
    )
    stock_data = []
    news_data = []
    timeout = 10  # 秒
    last_msg_time = time.time()
    while True:
        stock_pack = consumer_stock.poll(timeout_ms=1000)
        news_pack = consumer_news.poll(timeout_ms=1000)
        got_msg = False
        for tp, messages in stock_pack.items():
            for message in messages:
                stock_data.append(message.value)
                last_msg_time = time.time()
                got_msg = True
        for tp, messages in news_pack.items():
            for message in messages:
                news_data.append(message.value)
                last_msg_time = time.time()
                got_msg = True
        if not got_msg and time.time() - last_msg_time > timeout:
            break
    stock_df = pd.DataFrame(stock_data)
    news_df = pd.DataFrame(news_data)
    if stock_df.empty or news_df.empty:
        print("No data to join")
        return
    # 简单连接：基于 symbol
    joined_df = pd.merge(stock_df, news_df, on="symbol", how="inner")
    for _, row in joined_df.iterrows():
        joined_record = row.to_dict()
        producer.send(JOINED_TOPIC, joined_record)
        print(f"Joined record: {joined_record}")
    producer.flush()
    print(f"Produced {len(joined_df)} joined records to topic {JOINED_TOPIC}.")

if __name__ == "__main__":
    run()