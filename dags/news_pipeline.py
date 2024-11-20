from airflow import DAG, Dataset
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from src.tasks.news.fetch_task import NewsFetchOperator
from src.tasks.news.process_task import NewsProcessOperator
from src.tasks.news.sentiment_task import NewsSentimentOperator
from src.utils.health_check import HealthCheck
from src.utils.callbacks import task_failure_callback, dag_failure_callback, sla_miss_callback

news_dataset = Dataset("news://data/sentiment")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_failure_callback
}

with DAG(
    'news_sentiment_pipeline',
    default_args=default_args,
    schedule_interval='0 19 * * 1-5',
    catchup=False,
    on_failure_callback=dag_failure_callback,
    sla_miss_callback=sla_miss_callback
) as dag:

    # Add health check
    check_environment = PythonOperator(
        task_id='check_environment',
        python_callable=HealthCheck.check_news_environment
    )

    # Fetch news from both sources
    fetch_yourstory = NewsFetchOperator(
        task_id='fetch_yourstory',
        source='yourstory',
        urls=[
            "https://yourstory.com/2024/10/hdb-financial-services-files-draft-papers-with-seb",
            "https://yourstory.com/2024/08/hdfc-tech-innovators-2024"
        ]
    )

    fetch_finshots = NewsFetchOperator(
        task_id='fetch_finshots',
        source='finshots',
        urls=[
            "https://finshots.in/markets/the-changing-face-of-hdfc-life-the-insurance-industry/",
            "https://finshots.in/markets/what-to-make-of-hdfc-lifes-performance/"
        ]
    )

    # Process and clean news
    process_news = NewsProcessOperator(
        task_id='process_news',
        dag=dag
    )

    # Analyze sentiment
    analyze_sentiment = NewsSentimentOperator(
        task_id='analyze_sentiment',
        dag=dag
    )

    # Set dependencies
    check_environment >> [fetch_yourstory, fetch_finshots] >> process_news >> analyze_sentiment