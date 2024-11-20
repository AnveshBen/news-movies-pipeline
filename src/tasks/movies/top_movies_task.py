from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from src.utils.spark_manager import SparkManager
from src.config.settings import Settings
from pyspark.sql import functions as F
import logging

class TopMoviesOperator(BaseOperator):
    @apply_defaults
    def __init__(self, min_ratings=35, top_n=20, **kwargs):
        super().__init__(**kwargs)
        self.min_ratings = min_ratings
        self.top_n = top_n

    def execute(self, context):
        logger = logging.getLogger(__name__)
        spark = None
        try:
            spark = SparkManager.get_session()
            logger.info("Starting top movies analysis")

            # Read data from MinIO
            ratings_path = f"s3a://{Settings.MOVIES_BUCKET}/{Settings.MOVIE_PATHS['ratings']}"
            movies_path = f"s3a://{Settings.MOVIES_BUCKET}/{Settings.MOVIE_PATHS['movies']}"
            
            ratings_df = spark.read.csv(
                ratings_path,
                sep='\t',
                schema=SparkManager.RATINGS_SCHEMA
            )
            
            movies_df = spark.read.csv(
                movies_path,
                sep='|',
                schema=SparkManager.MOVIES_SCHEMA
            )

            # Calculate top movies
            top_movies = ratings_df.groupBy('item_id') \
                .agg(
                    F.count('*').alias('rating_count'),
                    F.avg('rating').alias('avg_rating')
                ) \
                .filter(F.col('rating_count') >= self.min_ratings) \
                .join(movies_df, ratings_df.item_id == movies_df.movie_id) \
                .select(
                    'title',
                    'avg_rating',
                    'rating_count'
                ) \
                .orderBy(F.col('avg_rating').desc()) \
                .limit(self.top_n)

            # Log results
            logger.info("\nTop 20 Highest Rated Movies (min. 35 ratings):")
            for row in top_movies.collect():
                logger.info(
                    f"{row['title']}: {row['avg_rating']:.2f} "
                    f"({row['rating_count']} ratings)"
                )

            return [row.asDict() for row in top_movies.collect()]

        except Exception as e:
            logger.error(f"Error in top movies task: {str(e)}")
            raise
        finally:
            if spark:
                SparkManager.stop_session() 