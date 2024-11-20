import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from src.config.settings import Settings

logger = logging.getLogger(__name__)

class SparkManager:
    _session = None

    # Define schemas based on actual data format
    RATINGS_SCHEMA = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("item_id", IntegerType(), True),  # movie id
        StructField("rating", FloatType(), True),
        StructField("timestamp", IntegerType(), True)
    ])
    
    MOVIES_SCHEMA = StructType([
        StructField("movie_id", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("release_date", StringType(), True),
        StructField("video_release_date", StringType(), True),
        StructField("imdb_url", StringType(), True),
        StructField("unknown", IntegerType(), True),
        StructField("Action", IntegerType(), True),
        StructField("Adventure", IntegerType(), True),
        StructField("Animation", IntegerType(), True),
        StructField("Children", IntegerType(), True),
        StructField("Comedy", IntegerType(), True),
        StructField("Crime", IntegerType(), True),
        StructField("Documentary", IntegerType(), True),
        StructField("Drama", IntegerType(), True),
        StructField("Fantasy", IntegerType(), True),
        StructField("Film_Noir", IntegerType(), True),
        StructField("Horror", IntegerType(), True),
        StructField("Musical", IntegerType(), True),
        StructField("Mystery", IntegerType(), True),
        StructField("Romance", IntegerType(), True),
        StructField("Sci_Fi", IntegerType(), True),
        StructField("Thriller", IntegerType(), True),
        StructField("War", IntegerType(), True),
        StructField("Western", IntegerType(), True)
    ])
    
    USERS_SCHEMA = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("age", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("occupation", StringType(), True),
        StructField("zip_code", StringType(), True)
    ])

    @classmethod
    def get_session(cls):
        if cls._session is None:
            logger = logging.getLogger(__name__)
            logger.info("Creating new SparkSession")
            
            try:
                # Create Spark session with proper configuration
                builder = SparkSession.builder \
                    .appName("MovieLens Analysis") \
                    .config("spark.driver.memory", "1g") \
                    .config("spark.executor.memory", "1g") \
                    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                    .config("spark.hadoop.fs.s3a.endpoint", f"http://{Settings.MINIO_ENDPOINT}") \
                    .config("spark.hadoop.fs.s3a.access.key", Settings.MINIO_ACCESS_KEY) \
                    .config("spark.hadoop.fs.s3a.secret.key", Settings.MINIO_SECRET_KEY) \
                    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                    .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                           "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

                # Create the session
                cls._session = builder.master("local[*]").getOrCreate()
                
                # Set log level
                cls._session.sparkContext.setLogLevel("WARN")
                
                logger.info("SparkSession created successfully")
                
            except Exception as e:
                logger.error(f"Error creating SparkSession: {str(e)}")
                raise
                
        return cls._session

    @classmethod
    def stop_session(cls):
        if cls._session:
            cls._session.stop()
            cls._session = None