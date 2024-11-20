from minio import Minio
from src.config.settings import Settings
import psycopg2
from psycopg2.pool import SimpleConnectionPool
import logging
from contextlib import contextmanager
from typing import List, Dict

class StorageManager:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.postgres_pool = None  # Initialize as None
        
        # Initialize MinIO client
        self.minio_client = Minio(
            Settings.MINIO_ENDPOINT,
            access_key=Settings.MINIO_ACCESS_KEY,
            secret_key=Settings.MINIO_SECRET_KEY,
            secure=Settings.MINIO_SECURE
        )
        
        # Initialize buckets and tables
        self._ensure_buckets()
        self._ensure_sentiment_table()

    def get_postgres_connection(self):
        """Get PostgreSQL connection using Settings"""
        return psycopg2.connect(Settings.get_postgres_uri())

    def _ensure_sentiment_table(self):
        """Ensure sentiment table exists"""
        try:
            with self.get_postgres_connection() as conn:  # Using the correct connection method
                with conn.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS article_sentiments (
                            id SERIAL PRIMARY KEY,
                            source VARCHAR(50),
                            ticker VARCHAR(50),
                            title TEXT,
                            sentiment_score FLOAT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                conn.commit()
        except Exception as e:
            self.logger.error(f"Error ensuring sentiment table: {str(e)}")
            raise

    def _ensure_buckets(self):
        """Ensure required MinIO buckets exist"""
        try:
            required_buckets = [Settings.NEWS_BUCKET, Settings.MOVIES_BUCKET]
            existing_buckets = [bucket.name for bucket in self.minio_client.list_buckets()]
            
            for bucket in required_buckets:
                if bucket not in existing_buckets:
                    self.minio_client.make_bucket(bucket)
                    self.logger.info(f"Created bucket: {bucket}")
        except Exception as e:
            self.logger.error(f"Error ensuring buckets: {str(e)}")
            raise

    def store_file(self, bucket: str, object_name: str, data, length: int = None):
        """Store a file in MinIO"""
        try:
            self.minio_client.put_object(
                bucket_name=bucket,
                object_name=object_name,
                data=data,
                length=length
            )
            self.logger.info(f"Stored file: {bucket}/{object_name}")
        except Exception as e:
            self.logger.error(f"Failed to store file {object_name}: {str(e)}")
            raise

    def get_file(self, bucket: str, object_name: str):
        """Retrieve a file from MinIO"""
        try:
            return self.minio_client.get_object(
                bucket_name=bucket,
                object_name=object_name
            )
        except Exception as e:
            self.logger.error(f"Failed to retrieve file {object_name}: {str(e)}")
            raise

    def execute_query(self, query: str, params: tuple = None, fetch: bool = False):
        """Execute a PostgreSQL query"""
        with self.get_postgres_connection() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(query, params)
                    if fetch:
                        return cur.fetchall()
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    self.logger.error(f"Query execution failed: {str(e)}")
                    raise

    def store_sentiment_results(self, articles: List[Dict]):
        """Store sentiment results in PostgreSQL"""
        try:
            with self.get_postgres_connection() as conn:
                with conn.cursor() as cur:
                    for article in articles:
                        cur.execute(
                            """
                            INSERT INTO article_sentiments 
                            (source, ticker, title, sentiment_score)
                            VALUES (%(source)s, %(ticker)s, %(title)s, %(sentiment_score)s)
                            """,
                            article
                        )
                    conn.commit()
            self.logger.info(f"Successfully stored {len(articles)} sentiment results")
        except Exception as e:
            self.logger.error(f"Error storing sentiment results: {str(e)}")
            raise

    def get_sentiment_results(self, limit: int = 100, offset: int = 0) -> List[Dict]:
        """Query sentiment results for analysis"""
        try:
            with self.get_postgres_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT source, ticker, title, sentiment_score, created_at
                        FROM article_sentiments
                        ORDER BY created_at DESC
                        LIMIT %s OFFSET %s
                        """,
                        (limit, offset)
                    )
                    columns = [desc[0] for desc in cur.description]
                    return [dict(zip(columns, row)) for row in cur.fetchall()]
        except Exception as e:
            self.logger.error(f"Error querying sentiment results: {str(e)}")
            raise

    def __del__(self):
        """Cleanup connections"""
        try:
            if hasattr(self, 'postgres_pool') and self.postgres_pool:
                self.postgres_pool.closeall()
        except Exception as e:
            self.logger.error(f"Error cleaning up connections: {str(e)}")