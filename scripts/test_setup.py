import sys
import logging
from src.utils.spark_manager import SparkManager
from src.utils.storage_manager import StorageManager
from src.utils.health_check import HealthCheck

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_setup():
    issues = []
    
    # Test 1: Spark Setup
    try:
        spark = SparkManager.get_session()
        df = spark.createDataFrame([(1, "test")], ["id", "value"])
        df.show()
        logger.info("✅ Spark setup successful")
    except Exception as e:
        issues.append(f"❌ Spark setup failed: {str(e)}")

    # Test 2: PostgreSQL Connection
    try:
        storage = StorageManager()
        with storage.get_postgres_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        logger.info("✅ PostgreSQL connection successful")
    except Exception as e:
        issues.append(f"❌ PostgreSQL connection failed: {str(e)}")

    # Test 3: MinIO Setup
    try:
        storage = StorageManager()
        buckets = storage.minio_client.list_buckets()
        logger.info("✅ MinIO setup successful")
    except Exception as e:
        issues.append(f"❌ MinIO setup failed: {str(e)}")

    return issues

if __name__ == "__main__":
    logger.info("Starting setup test...")
    issues = test_setup()
    
    if issues:
        logger.error("Setup test failed:")
        for issue in issues:
            logger.error(issue)
        sys.exit(1)
    else:
        logger.info("All tests passed successfully!") 