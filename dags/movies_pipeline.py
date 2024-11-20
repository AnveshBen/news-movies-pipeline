from airflow import DAG, Dataset
from datetime import datetime, timedelta
from src.tasks.movies.fetch_dataset_task import MovieLensDatasetOperator
from src.tasks.movies.age_analysis_task import OccupationAgeAnalysisOperator
from src.tasks.movies.top_movies_task import TopMoviesOperator
from src.tasks.movies.genre_analysis_task import GenreAnalysisOperator
from src.tasks.movies.similarity_task import MovieSimilarityOperator
from src.tasks.movies.store_results_task import StoreResultsOperator
from src.utils.health_check import HealthCheck
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator
from airflow.utils.context import Context
from src.utils.callbacks import task_failure_callback, dag_failure_callback, sla_miss_callback

def get_news_execution_date(execution_date):
    """
    Get the execution date for the news pipeline that should have run today
    Returns the start of the current day
    """
    return execution_date.replace(hour=0, minute=0, second=0, microsecond=0)

def choose_path(**kwargs) -> str:
    """
    Choose which path to take based on run type
    Returns the next task ID
    """
    is_manual = kwargs['dag_run'].external_trigger
    if is_manual:
        return 'start_processing'  # Skip news dependency for manual runs
    return 'wait_for_news_pipeline'  # Check news dependency for scheduled runs

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_failure_callback
}

with DAG(
    'movies_analysis_pipeline',
    default_args=default_args,
    schedule='0 20 * * 1-5',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    on_failure_callback=dag_failure_callback,
    sla_miss_callback=sla_miss_callback
) as dag:

    # Branch operator to choose path
    choose_branch = BranchPythonOperator(
        task_id='choose_branch',
        python_callable=choose_path,
    )

    # Empty operator as a join point
    start_processing = EmptyOperator(
        task_id='start_processing',
        trigger_rule='none_failed'
    )

    # Wait for news pipeline completion
    wait_for_news = ExternalTaskSensor(
        task_id='wait_for_news_pipeline',
        external_dag_id='news_sentiment_pipeline',
        external_task_id='analyze_sentiment',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        execution_date_fn=get_news_execution_date,
        timeout=1800,
        mode='reschedule',
        poke_interval=60,
        soft_fail=True
    )

    # Rest of your tasks...
    check_environment = PythonOperator(
        task_id='check_environment',
        python_callable=HealthCheck.check_movies_environment
    )

    fetch_dataset = MovieLensDatasetOperator(
        task_id='fetch_dataset',
        dataset_url="https://files.grouplens.org/datasets/movielens/ml-100k.zip"
    )

    age_analysis = OccupationAgeAnalysisOperator(
        task_id='occupation_age_analysis'
    )

    top_movies = TopMoviesOperator(
        task_id='top_movies',
        min_ratings=35,
        top_n=20
    )

    genre_analysis = GenreAnalysisOperator(
        task_id='genre_analysis'
    )

    movie_similarity = MovieSimilarityOperator(
        task_id='movie_similarity',
        movie_title='Usual Suspects, The (1995)',
        dag=dag
    )

    store_results = StoreResultsOperator(
        task_id='store_results'
    )

    # Set up branching task dependencies
    choose_branch >> [wait_for_news, start_processing]
    wait_for_news >> start_processing
    
    # Rest of the pipeline
    start_processing >> check_environment >> fetch_dataset
    
    fetch_dataset >> [
        age_analysis,
        top_movies,
        genre_analysis,
        movie_similarity
    ] >> store_results