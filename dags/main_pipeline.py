from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG(
    'main_pipeline',
    default_args=default_args,
    description='Stock & News Streaming Pipeline',
    schedule_interval='*/10 * * * *',  # 每10分钟跑一次
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # 股票采集
    stock_producer = BashOperator(
        task_id='run_alpha_vantage_producer',
        bash_command='python /opt/airflow/producers/alpha_vantage_producer.py'
    )

    # 新闻采集
    news_producer = BashOperator(
        task_id='run_newsapi_producer',
        bash_command='python /opt/airflow/producers/newsapi_producer.py'
    )

    # Spark 格式化（股票）
    spark_format_stock = BashOperator(
        task_id='spark_format_stock',
        bash_command='spark-submit --master local /opt/airflow/spark_jobs/format_stock.py'
    )

    # Spark 格式化（新闻）
    spark_format_news = BashOperator(
        task_id='spark_format_news',
        bash_command='spark-submit --master local /opt/airflow/spark_jobs/format_news.py'
    )

    # （可选）Spark join分析
    # spark_join = BashOperator(
    #     task_id='spark_join_stock_news',
    #     bash_command='spark-submit --master local /opt/airflow/spark_jobs/join_stock_news.py'
    # )

    # Kafka → ES
    result_to_es = BashOperator(
        task_id='result_to_es',
        bash_command='python /opt/airflow/consumers/result_to_es.py'
    )

    # 任务依赖关系
    [stock_producer, news_producer] >> [spark_format_stock, spark_format_news] >> result_to_es