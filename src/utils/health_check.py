from src.utils.storage_manager import StorageManager
from src.utils.spark_manager import SparkManager
import psycopg2
import logging
import os
from airflow.exceptions import AirflowException
from src.config.settings import Settings

class HealthCheck:
    @staticmethod
    def check_all_services():
        """Check all required services"""
        issues = []
        
        # Check PostgreSQL
        if not HealthCheck.check_postgres():
            issues.append("PostgreSQL connection failed")
            
        # Check MinIO
        if not HealthCheck.check_minio():
            issues.append("MinIO connection failed")
            
        # Check Spark
        if not HealthCheck.check_spark():
            issues.append("Spark initialization failed")
            
        return issues
    
    @staticmethod
    def check_postgres():
        try:
            storage = StorageManager()
            with storage.get_postgres_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            return True
        except Exception as e:
            logging.error(f"PostgreSQL health check failed: {str(e)}")
            return False
            
    @staticmethod
    def check_minio():
        try:
            storage = StorageManager()
            storage.minio_client.list_buckets()
            return True
        except Exception as e:
            logging.error(f"MinIO health check failed: {str(e)}")
            return False
            
    @staticmethod
    def check_spark():
        try:
            spark = SparkManager.get_session()
            spark.sql("SELECT 1").collect()
            return True
        except Exception as e:
            logging.error(f"Spark health check failed: {str(e)}")
            return False
            
    @staticmethod
    def check_environment():
        logger = logging.getLogger(__name__)
        required_vars = ['JAVA_HOME', 'SPARK_HOME', 'PYTHONPATH']
        
        for var in required_vars:
            if not os.environ.get(var):
                raise AirflowException(f"Required environment variable {var} is not set")
                
        # Check data directory
        data_dir = '/opt/airflow/data/ml-100k'
        if not os.path.exists(data_dir):
            raise AirflowException(f"Data directory not found: {data_dir}")
            
        required_files = ['u.user', 'u.data', 'u.item']
        for file in required_files:
            file_path = os.path.join(data_dir, file)
            if not os.path.exists(file_path):
                raise AirflowException(f"Required data file not found: {file_path}")
        
        logger.info("Environment check passed successfully")
        return True 

    @staticmethod
    def check_news_environment():
        logger = logging.getLogger(__name__)
        required_vars = ['JAVA_HOME', 'SPARK_HOME', 'PYTHONPATH']
        
        for var in required_vars:
            if not os.environ.get(var):
                raise AirflowException(f"Required environment variable {var} is not set")

        # Check API credentials if needed
        # Add any specific news pipeline checks here
        
        logger.info("News pipeline environment check passed successfully")
        return True

    @staticmethod
    def check_movies_environment():
        logger = logging.getLogger(__name__)
        # Previous movies environment check code
        ...

def check_minio_connection():
    """Check MinIO connection and bucket existence"""
    logger = logging.getLogger(__name__)
    try:
        storage = StorageManager()
        
        # Check buckets
        for bucket in [Settings.MOVIES_BUCKET, Settings.NEWS_BUCKET]:
            if storage.minio_client.bucket_exists(bucket):
                logger.info(f"Bucket '{bucket}' exists")
            else:
                logger.warning(f"Bucket '{bucket}' does not exist")
                
        return True
    except Exception as e:
        logger.error(f"MinIO health check failed: {str(e)}")
        return False