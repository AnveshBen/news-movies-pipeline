import os
from typing import Dict

class Settings:
    """Configuration settings for the application"""
    
    # MinIO Configuration
    MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
    MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
    MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
    MINIO_SECURE = os.getenv('MINIO_SECURE', 'False').lower() == 'true'

    # PostgreSQL Configuration
    POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres')
    POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', '5432'))
    POSTGRES_DB = os.getenv('POSTGRES_DB', 'airflow')
    POSTGRES_USER = os.getenv('POSTGRES_USER', 'airflow')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'airflow')

    # Bucket Names
    MOVIES_BUCKET = 'movielens'
    NEWS_BUCKET = 'news'

    # File Paths
    MOVIE_PATHS: Dict[str, str] = {
        'ratings': 'ml-100k/u.data',
        'movies': 'ml-100k/u.item',
        'users': 'ml-100k/u.user'
    }

    NEWS_PATHS: Dict[str, str] = {
        'articles': 'articles.json',
        'sentiment': 'sentiment_results.json',
        'analysis': 'analysis_results.json'
    }

    NEWS_SOURCES = {
        'yourstory': 'https://yourstory.com',
        'finshots': 'https://finshots.in'
    }

    # Adding ticker configuration for clarity
    NEWS_TICKERS = ['HDFC', 'Tata Motors']
    ARTICLES_PER_TICKER = 5

    # Schedule configuration (7 PM every working day)
    NEWS_PIPELINE_SCHEDULE = '0 19 * * 1-5'  # Monday to Friday at 7 PM
    MOVIES_PIPELINE_SCHEDULE = '0 20 * * 1-5'  # Monday to Friday at 8 PM

    # Add browserless configuration
    BROWSERLESS_URL = os.getenv('BROWSERLESS_URL', 'browserless:3000')

    @classmethod
    def get_movie_path(cls, key: str) -> str:
        """Get full path for movie files"""
        return f"{cls.MOVIES_BUCKET}/{cls.MOVIE_PATHS[key]}"

    @classmethod
    def get_news_path(cls, key: str) -> str:
        """Get full path for news files"""
        return f"{cls.NEWS_BUCKET}/{cls.NEWS_PATHS[key]}"

    @classmethod
    def get_spark_config(cls) -> Dict[str, str]:
        """Get Spark configuration for MinIO"""
        return {
            "spark.hadoop.fs.s3a.endpoint": f"http://{cls.MINIO_ENDPOINT}",
            "spark.hadoop.fs.s3a.access.key": cls.MINIO_ACCESS_KEY,
            "spark.hadoop.fs.s3a.secret.key": cls.MINIO_SECRET_KEY,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
        }

    @classmethod
    def get_postgres_uri(cls) -> str:
        """Get PostgreSQL connection URI"""
        return f"postgresql://{cls.POSTGRES_USER}:{cls.POSTGRES_PASSWORD}@{cls.POSTGRES_HOST}:{cls.POSTGRES_PORT}/{cls.POSTGRES_DB}"