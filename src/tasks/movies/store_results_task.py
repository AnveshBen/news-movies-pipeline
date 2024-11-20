from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from src.utils.storage_manager import StorageManager
from src.config.settings import Settings
import json
import logging
import io

class StoreResultsOperator(BaseOperator):
    @apply_defaults
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.storage = StorageManager()

    def execute(self, context):
        logger = logging.getLogger(__name__)
        try:
            # Get results from XCom
            task_ids = ['top_movies', 'genre_analysis', 'occupation_age_analysis', 'movie_similarity']
            results = {}
            
            for task_id in task_ids:
                results[task_id] = context['task_instance'].xcom_pull(task_ids=task_id)

            # Store results in MinIO
            results_json = json.dumps(results, indent=2)
            self.storage.minio_client.put_object(
                bucket_name=Settings.MOVIES_BUCKET,
                object_name='analysis_results.json',
                data=io.BytesIO(results_json.encode()),
                length=len(results_json)
            )

            logger.info("Analysis results stored successfully")
            return "Results stored successfully"

        except Exception as e:
            logger.error(f"Error storing results: {str(e)}")
            raise