from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from src.utils.storage_manager import StorageManager
from src.config.settings import Settings
import requests
import zipfile
import io
import logging

class MovieLensDatasetOperator(BaseOperator):
    """
    Operator to fetch and prepare MovieLens dataset
    """
    
    @apply_defaults
    def __init__(
        self,
        dataset_url: str = "https://files.grouplens.org/datasets/movielens/ml-100k.zip",
        **kwargs
    ):
        super().__init__(**kwargs)
        self.dataset_url = dataset_url
        self.storage = StorageManager()
        
    def execute(self, context):
        logger = logging.getLogger(__name__)
        
        try:
            # Download dataset
            logger.info(f"Downloading dataset from {self.dataset_url}")
            response = requests.get(self.dataset_url)
            response.raise_for_status()

            # Extract files from zip
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                for filename in ['u.data', 'u.item', 'u.user']:
                    with z.open(f'ml-100k/{filename}') as f:
                        # Read the file content
                        content = f.read()
                        
                        # Store in MinIO
                        self.storage.minio_client.put_object(
                            bucket_name=Settings.MOVIES_BUCKET,
                            object_name=f'ml-100k/{filename}',
                            data=io.BytesIO(content),
                            length=len(content)
                        )
                        logger.info(f"Stored {filename} in MinIO (size: {len(content)} bytes)")

            logger.info("Dataset loaded successfully")
            return "Dataset loaded successfully"

        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading dataset: {str(e)}")
            raise
        except zipfile.BadZipFile as e:
            logger.error(f"Error extracting zip file: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error loading dataset: {str(e)}")
            raise