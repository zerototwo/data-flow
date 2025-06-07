from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

def run_alpha_vantage_producer():
    subprocess.run(["python", "producers/alpha_vantage_producer.py"], check=True)

def run_newsapi_producer():
    subprocess.run(["python", "producers/newsapi_producer.py"], check=True)

def spark_format_stock():
    subprocess.run(["python", "spark_jobs/format_stock.py"], check=True)

def spark_format_news():
    subprocess.run(["python", "spark_jobs/format_news.py"], check=True)

def join_stock_news():
    subprocess.run(["python", "spark_jobs/join_stock_news.py"], check=True)

def result_to_es():
    subprocess.run(["python", "consumers/result_to_es.py"], check=True)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    'main_pipeline',
    default_args=default_args,
    description='DAG',
    schedule_interval='*/10 * * * *',
    start_date=datetime(2024, 6, 7),
    catchup=False,
)
run_alpha_vantage = PythonOperator(
    task_id='run_alpha_vantage_producer',
    python_callable=run_alpha_vantage_producer,
    dag=dag,
)
run_newsapi = PythonOperator(
    task_id='run_newsapi_producer',
    python_callable=run_newsapi_producer,
    dag=dag,
)
spark_stock = PythonOperator(
    task_id='spark_format_stock',
    python_callable=spark_format_stock,
    dag=dag,
)
spark_news = PythonOperator(
    task_id='spark_format_news',
    python_callable=spark_format_news,
    dag=dag,
)
join_task = PythonOperator(
    task_id='join_stock_news',
    python_callable=join_stock_news,
    dag=dag,
)
result_to_es_task = PythonOperator(
    task_id='result_to_es',
    python_callable=result_to_es,
    dag=dag,
)
# 依赖关系
run_alpha_vantage >> spark_stock
run_alpha_vantage >> spark_news
run_newsapi >> spark_stock
run_newsapi >> spark_news
spark_stock >> join_task
spark_news >> join_task
join_task >> result_to_es_task