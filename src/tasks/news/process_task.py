from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp, length
from pyspark.sql.types import StringType
import re
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

class NewsProcessOperator(BaseOperator):
    """
    Custom operator for processing financial news data using PySpark
    """
    
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            nltk.data.find('tokenizers/punkt')
            nltk.data.find('corpora/stopwords')
        except LookupError:
            nltk.download('punkt')
            nltk.download('stopwords')

    def preprocess_text(self, text):
        if not text or not isinstance(text, str):
            return ""
            
        # Convert to lowercase
        text = text.lower()
        
        # Remove URLs
        text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
        
        # Remove HTML tags
        text = re.sub(r'<.*?>', '', text)
        
        # Replace currency symbols
        text = re.sub(r'\$', ' dollar ', text)
        text = re.sub(r'€', ' euro ', text)
        text = re.sub(r'£', ' pound ', text)
        
        # Standardize numbers
        text = re.sub(r'\b\d+%', lambda x: x.group()[:-1] + ' percent', text)
        text = re.sub(r'\b\d+k\b', lambda x: x.group()[:-1] + ' thousand', text)
        text = re.sub(r'\b\d+m\b', lambda x: x.group()[:-1] + ' million', text)
        text = re.sub(r'\b\d+b\b', lambda x: x.group()[:-1] + ' billion', text)
        
        # Keep numbers and basic punctuation
        text = re.sub(r'[^\w\s.,!?$%\-]', ' ', text)
        
        # Remove extra whitespace
        text = ' '.join(text.split())
        
        return text.strip()

    def execute(self, context):
        """
        Process the ticker data using PySpark
        """
        self.log.info("Starting news processing task")
        task_instance = context['task_instance']
        
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("NewsProcessing") \
            .getOrCreate()

        try:
            # Pull data from both sources
            yourstory_data = task_instance.xcom_pull(task_ids='fetch_yourstory') or []
            finshots_data = task_instance.xcom_pull(task_ids='fetch_finshots') or []
            
            # Debug logging
            if yourstory_data:
                self.log.info(f"Sample YourStory article structure: {yourstory_data[0].keys() if yourstory_data else 'No data'}")
            if finshots_data:
                self.log.info(f"Sample Finshots article structure: {finshots_data[0].keys() if finshots_data else 'No data'}")
            
            # Combine data
            ticker_data = yourstory_data + finshots_data
            
            self.log.info(f"Received {len(yourstory_data)} articles from YourStory")
            self.log.info(f"Received {len(finshots_data)} articles from Finshots")
            self.log.info(f"Total articles to process: {len(ticker_data)}")
            
            if not ticker_data:
                return []

            # Create Spark DataFrame
            df = spark.createDataFrame(ticker_data)
            
            # Register UDF for text preprocessing
            preprocess_udf = udf(self.preprocess_text, StringType())
            
            # Apply preprocessing
            processed_df = df \
                .withColumn("title_processed", preprocess_udf(col("title"))) \
                .withColumn("text_processed", preprocess_udf(col("text"))) \
                .withColumn("title_original", col("title")) \
                .withColumn("text_original", col("text")) \
                .withColumn("published", to_timestamp(col("published")))
            
            # Remove empty processed text
            processed_df = processed_df \
                .filter(length(col("title_processed")) > 0) \
                .filter(length(col("text_processed")) > 0)
            
            # Remove duplicates
            processed_df = processed_df.dropDuplicates(["title", "text"])
            
            # Convert to pandas and prepare for XCom
            pandas_df = processed_df.toPandas()
            
            # Convert timestamps to ISO format strings
            pandas_df['published'] = pandas_df['published'].dt.strftime('%Y-%m-%dT%H:%M:%S%z')
            
            # Convert to list of dictionaries
            processed_data = pandas_df.to_dict('records')
            
            self.log.info(f"Successfully processed {len(processed_data)} news articles")
            
            # Stop Spark session
            spark.stop()
            
            return processed_data

        except Exception as e:
            self.log.error(f"Error in NewsProcessOperator: {str(e)}")
            self.log.exception("Full error traceback:")
            if 'spark' in locals():
                spark.stop()
            return [] 