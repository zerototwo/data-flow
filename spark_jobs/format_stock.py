import json
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

    for message in consumer_stock:
        stock_data.append(message.value)
        if len(stock_data) >= 10:
            break

    for message in consumer_news:
        news_data.append(message.value)
        if len(news_data) >= 10:
            break

    stock_df = pd.DataFrame(stock_data)
    news_df = pd.DataFrame(news_data)

    if stock_df.empty or news_df.empty:
        print("No data to join")
        return

    # 简单连接：基于 symbol，实际应用中可按时间戳近似关联
    joined_df = pd.merge(stock_df, news_df, on="symbol", how="inner")

    for _, row in joined_df.iterrows():
        joined_record = row.to_dict()
        producer.send(JOINED_TOPIC, joined_record)
        print(f"Joined record: {joined_record}")