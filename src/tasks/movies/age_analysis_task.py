import os
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from src.utils.spark_manager import SparkManager
from src.config.settings import Settings
from pyspark.sql import functions as F
import logging

class OccupationAgeAnalysisOperator(BaseOperator):
    @apply_defaults
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context):
        logger = logging.getLogger(__name__)
        spark = None
        try:
            data_path = f"s3a://{Settings.MOVIES_BUCKET}/{Settings.MOVIE_PATHS['users']}"
            logger.info(f"Reading user data from MinIO: {data_path}")
                
            spark = SparkManager.get_session()
            
            users_df = spark.read.csv(
                data_path,
                sep='|',
                schema=SparkManager.USERS_SCHEMA
            )
            
            # Calculate mean age by occupation
            occupation_stats = users_df.groupBy('occupation') \
                .agg(F.mean('age').alias('mean_age')) \
                .orderBy('occupation')
            
            # Log results
            logger.info("\nMean Age by Occupation:")
            for row in occupation_stats.collect():
                logger.info(f"{row['occupation']}: {row['mean_age']:.1f}")
            
            return {
                'occupation_stats': [row.asDict() for row in occupation_stats.collect()]
            }
            
        except Exception as e:
            logger.error(f"Error in age analysis task: {str(e)}")
            raise
        finally:
            if spark:
                SparkManager.stop_session()