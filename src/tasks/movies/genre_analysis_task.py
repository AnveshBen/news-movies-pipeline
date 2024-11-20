from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from src.utils.spark_manager import SparkManager
from src.config.settings import Settings
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import logging

class GenreAnalysisOperator(BaseOperator):
    @apply_defaults
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context):
        logger = logging.getLogger(__name__)
        spark = None
        try:
            spark = SparkManager.get_session()
            logger.info("Starting genre analysis task")

            # Read and validate data from MinIO
            ratings_df = spark.read.csv(
                f"s3a://{Settings.MOVIES_BUCKET}/{Settings.MOVIE_PATHS['ratings']}", 
                sep='\t', 
                schema=SparkManager.RATINGS_SCHEMA
            ).cache()
            
            movies_df = spark.read.csv(
                f"s3a://{Settings.MOVIES_BUCKET}/{Settings.MOVIE_PATHS['movies']}", 
                sep='|', 
                schema=SparkManager.MOVIES_SCHEMA
            ).cache()
            
            users_df = spark.read.csv(
                f"s3a://{Settings.MOVIES_BUCKET}/{Settings.MOVIE_PATHS['users']}", 
                sep='|', 
                schema=SparkManager.USERS_SCHEMA
            ).cache()

            # Create age groups
            users_grouped = users_df.filter(F.col('age').isNotNull()) \
                .withColumn(
                    'age_group',
                    F.when((F.col('age') >= 20) & (F.col('age') < 25), '20-25')
                    .when((F.col('age') >= 25) & (F.col('age') < 35), '25-35')
                    .when((F.col('age') >= 35) & (F.col('age') < 45), '35-45')
                    .otherwise('45 and older')
                )

            # Get genres from schema (excluding non-genre columns)
            non_genre_cols = ['movie_id', 'title', 'release_date', 
                            'video_release_date', 'imdb_url', 'unknown']
            genre_columns = [field.name for field in movies_df.schema.fields 
                           if field.name not in non_genre_cols]

            logger.info(f"Found genres: {genre_columns}")

            # Create genre-movie mapping
            genre_movie = movies_df.select(
                F.col('movie_id'),
                F.explode(
                    F.array(*[
                        F.when(F.col(genre) == 1, F.lit(genre))
                        .otherwise(None)
                        for genre in genre_columns
                    ])
                ).alias('genre')
            ).where(F.col('genre').isNotNull())

            # Join and analyze
            joined_data = ratings_df \
                .join(genre_movie, ratings_df.item_id == genre_movie.movie_id) \
                .join(users_grouped, ratings_df.user_id == users_grouped.user_id)

            # Calculate genre preferences
            result = joined_data \
                .groupBy('occupation', 'age_group', 'genre') \
                .agg(
                    F.avg('rating').alias('avg_rating'),
                    F.count('*').alias('rating_count')
                ) \
                .withColumn(
                    'rank',
                    F.row_number().over(
                        Window.partitionBy('occupation', 'age_group')
                        .orderBy(F.desc('avg_rating'), F.desc('rating_count'))
                    )
                ) \
                .filter(F.col('rank') == 1) \
                .orderBy('occupation', 'age_group')

            # Log results
            logger.info("\n=== Top Genre by Occupation and Age Group ===")
            results = result.collect()
            
            current_occupation = None
            for row in results:
                if current_occupation != row['occupation']:
                    current_occupation = row['occupation']
                    logger.info(f"\nOccupation: {current_occupation}")
                logger.info(
                    f"  Age Group {row['age_group']:12} : {row['genre']:12} "
                    f"(Avg Rating: {row['avg_rating']:.2f}, Count: {row['rating_count']})"
                )

            # Cleanup
            ratings_df.unpersist()
            movies_df.unpersist()
            users_df.unpersist()

            return [row.asDict() for row in results]

        except Exception as e:
            logger.error(f"Error in genre analysis task: {str(e)}")
            raise
        finally:
            if spark:
                SparkManager.stop_session() 