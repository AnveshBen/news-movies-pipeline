class DataQualityChecker:
    @staticmethod
    def validate_movie_data(df):
        """Validate movie analysis results"""
        checks = {
            'null_check': df.count() == df.dropna().count(),
            'rating_range': df.filter((df.rating < 0) | (df.rating > 5)).count() == 0,
            'unique_movies': df.select('movie_id').distinct().count() == df.count()
        }
        return all(checks.values()), checks 