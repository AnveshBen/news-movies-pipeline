from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from src.utils.spark_manager import SparkManager
from src.config.settings import Settings
from pyspark.sql import functions as F
import logging

class MovieSimilarityOperator(BaseOperator):
    @apply_defaults
    def __init__(self, movie_title: str, 
                 similarity_threshold: float = 0.5,
                 min_common_users: int = 50,
                 top_n: int = 10,
                 **kwargs):
        super().__init__(**kwargs)
        self.movie_title = movie_title
        self.similarity_threshold = similarity_threshold
        self.min_common_users = min_common_users
        self.top_n = top_n

    def get_movie_id(self, movie_title: str, spark) -> int:
        """Get movie ID from title"""
        movies_df = spark.read.csv(
            f"s3a://{Settings.MOVIES_BUCKET}/{Settings.MOVIE_PATHS['movies']}",
            sep='|',
            schema=SparkManager.MOVIES_SCHEMA
        )
        
        # Add diagnostic for movie search
        matching_movies = movies_df.filter(
            F.lower(F.col('title')).like(f"%{movie_title.lower()}%")
        ).collect()
        
        if len(matching_movies) > 0:
            self.log.info("Found matching movies:")
            for m in matching_movies:
                self.log.info(f"- {m.title}")
        
        movie = movies_df.filter(F.col('title') == movie_title).first()
        if not movie:
            raise ValueError(f"Movie not found: {movie_title}")
            
        self.log.info(f"Found movie ID {movie.movie_id} for '{movie_title}'")
        return movie.movie_id

    def calculate_similarity(self, paired_ratings):
        """
        Calculate similarity between movies based on how users rate pairs of movies.
        For each pair of movies (target movie and another movie):
        1. Find users who rated both movies
        2. Compare their ratings using Pearson correlation
        """
        # Log the initial paired ratings count
        total_pairs = paired_ratings.count()
        self.log.info(f"Found {total_pairs} paired ratings from users who rated both movies")

        # Group by movie and calculate similarity
        similarity_metrics = paired_ratings.groupBy('other_item_id').agg(
            # Pearson correlation between how users rate both movies
            F.corr('target_rating', 'other_rating').alias('pearson_score'),
            # Count how many users rated both movies
            F.count('*').alias('strength')
        )

        # Log similarity distribution statistics
        similarity_stats = similarity_metrics.select(
            F.count('*').alias('total_compared_movies'),
            F.count(F.when(F.col('pearson_score').isNotNull(), True)).alias('valid_comparisons'),
            F.avg('pearson_score').alias('avg_similarity'),
            F.min('strength').alias('min_common_users'),
            F.max('strength').alias('max_common_users')
        ).collect()[0]
        
        self.log.info(f"""
        Similarity Analysis Results:
        - Total movies compared: {similarity_stats['total_compared_movies']}
        - Valid comparisons: {similarity_stats['valid_comparisons']}
        - Average similarity score: {similarity_stats['avg_similarity']:.4f}
        - Min common users: {similarity_stats['min_common_users']}
        - Max common users: {similarity_stats['max_common_users']}
        """)

        return similarity_metrics

    def execute(self, context):
        logger = logging.getLogger(__name__)
        spark = None
        try:
            spark = SparkManager.get_session()
            logger.info(f"Starting similarity analysis for movie: {self.movie_title}")

            # Load ratings data
            ratings_df = spark.read.csv(
                f"s3a://{Settings.MOVIES_BUCKET}/{Settings.MOVIE_PATHS['ratings']}",
                sep='\t',
                schema=SparkManager.RATINGS_SCHEMA
            ).cache()

            # Get target movie ID
            target_movie_id = self.get_movie_id(self.movie_title, spark)
            
            # Get users who rated the target movie and their ratings
            target_ratings = ratings_df.filter(
                F.col('item_id') == target_movie_id
            ).select(
                F.col('user_id').alias('target_user_id'),
                F.col('rating').alias('target_rating')
            ).cache()
            
            # Get all other movie ratings
            other_ratings = ratings_df.select(
                F.col('user_id').alias('other_user_id'),
                F.col('item_id').alias('other_item_id'),
                F.col('rating').alias('other_rating')
            ).cache()
            
            # Find pairs of ratings where the same user rated both movies
            paired_ratings = target_ratings.join(
                other_ratings,
                F.col('target_user_id') == F.col('other_user_id')
            ).filter(
                F.col('other_item_id') != target_movie_id
            ).cache()
            
            # Calculate similarities
            similarity_metrics = self.calculate_similarity(paired_ratings)
            
            # Get similar movies meeting thresholds
            movies_df = spark.read.csv(
                f"s3a://{Settings.MOVIES_BUCKET}/{Settings.MOVIE_PATHS['movies']}",
                sep='|',
                schema=SparkManager.MOVIES_SCHEMA
            ).cache()

            similar_movies = similarity_metrics.filter(
                (F.col('strength') >= self.min_common_users) &  # At least 50 users rated both
                (F.col('pearson_score') >= self.similarity_threshold)  # 95% similar
            ).join(
                movies_df,
                similarity_metrics.other_item_id == movies_df.movie_id
            ).select(
                'title',
                F.round(F.col('pearson_score'), 16).alias('score'),
                'strength'
            ).orderBy(
                F.desc('score'),
                F.desc('strength')
            ).limit(self.top_n)

            # Log results
            results = similar_movies.collect()
            if len(results) == 0:
                logger.warning("No similar movies found meeting the criteria")
            else:
                logger.info(f"\nTop {self.top_n} similar movies for {self.movie_title}")
                for row in results:
                    logger.info(
                        f"{row['title']:<50} "
                        f"score: {row['score']:<50} "
                        f"strength: {row['strength']}"
                    )

            # Cleanup
            ratings_df.unpersist()
            movies_df.unpersist()
            target_ratings.unpersist()
            other_ratings.unpersist()
            paired_ratings.unpersist()

            return [row.asDict() for row in results]

        except Exception as e:
            logger.error(f"Error in movie similarity task: {str(e)}")
            raise
        finally:
            if spark:
                SparkManager.stop_session() 