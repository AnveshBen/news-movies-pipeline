from airflow.models import BaseOperator
from src.utils.storage_manager import StorageManager
from src.utils.mock_sentiment_api import get_sentiment_score
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
import time
import random
from datetime import datetime

class NewsSentimentOperator(BaseOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema = StructType([
            StructField("title", StringType(), True),
            StructField("text", StringType(), True),
            StructField("published", StringType(), True),
            StructField("url", StringType(), True),
            StructField("source", StringType(), True),
            StructField("title_processed", StringType(), True),
            StructField("text_processed", StringType(), True)
        ])

    def mock_api_call(self, text: str) -> float:
        """
        Simulate an API call with realistic behavior
        Returns: float (sentiment score between 0 and 1)
        """
        try:
            # Simulate API latency (100-500ms)
            time.sleep(random.uniform(0.1, 0.5))
            
            # Get sentiment score from mock API
            return float(get_sentiment_score(text))
            
        except Exception as e:
            self.log.warning(f"API call failed: {str(e)}, retrying...")
            # Retry once after failure
            try:
                time.sleep(1)  # Wait 1 second before retry
                return float(get_sentiment_score(text))
            except:
                return 0.5  # Neutral score on failure

    def execute(self, context):
        """
        Analyze sentiment of processed news articles
        """
        self.log.info("Analyzing sentiment")
        task_instance = context['task_instance']
        processed_news = task_instance.xcom_pull(task_ids='process_news')
        
        if not processed_news:
            self.log.warning("No processed news data received")
            return []
        
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("NewsSentimentAnalysis") \
            .getOrCreate()

        try:
            # Create Spark DataFrame
            df = spark.createDataFrame(processed_news, self.schema)
            
            # Convert string timestamp to proper timestamp
            df = df.withColumn("published", to_timestamp(col("published")))
            
            # Register UDF for sentiment analysis
            sentiment_udf = udf(self.mock_api_call, FloatType())
            
            # Apply sentiment analysis
            result_df = df \
                .withColumn("combined_text", 
                    col("title_processed") + " " + col("text_processed")) \
                .withColumn("sentiment_score", sentiment_udf(col("combined_text"))) \
                .select(
                    "source",
                    "title",
                    col("sentiment_score"),
                    lit("HDFC").alias("ticker")  # Default ticker
                )
            
            # Convert to list of dictionaries for storage
            results = result_df.toPandas().to_dict('records')
            
            self.log.info(f"Completed sentiment analysis for {len(results)} articles")
            
            # Store results
            storage_manager = StorageManager()
            storage_manager.store_sentiment_results(results)
            self.log.info(f"Successfully stored {len(results)} sentiment results")
            
            # Cleanup
            spark.stop()
            
            return results

        except Exception as e:
            self.log.error(f"Error in NewsSentimentOperator: {str(e)}")
            self.log.exception("Full error traceback:")
            if 'spark' in locals():
                spark.stop()
            return []