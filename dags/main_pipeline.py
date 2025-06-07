from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain
from datetime import datetime, timedelta

# DAG 定义
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

# 空任务节点定义
run_alpha_vantage = EmptyOperator(
    task_id='run_alpha_vantage_producer',
    dag=dag,
)

run_newsapi = EmptyOperator(
    task_id='run_newsapi_producer',
    dag=dag,
)

spark_stock = EmptyOperator(
    task_id='spark_format_stock',
    dag=dag,
)

spark_news = EmptyOperator(
    task_id='spark_format_news',
    dag=dag,
)

consume_stock_task = EmptyOperator(
    task_id='consume_stock',
    dag=dag,
)

consume_news_task = EmptyOperator(
    task_id='consume_news',
    dag=dag,
)

merge_to_es = EmptyOperator(
    task_id='merge_to_es',
    dag=dag,
)

# 正确设置依赖关系
chain(
    [run_alpha_vantage, run_newsapi],
    [spark_stock, spark_news],
    [consume_stock_task, consume_news_task]
)
consume_stock_task >> merge_to_es
consume_news_task >> merge_to_es