from datetime import datetime
import logging
from src.utils.storage_manager import StorageManager

class MetricsCollector:
    @staticmethod
    def record_task_metrics(task_id, start_time, end_time, status, error=None):
        """Record task execution metrics"""
        duration = (end_time - start_time).total_seconds()
        
        with StorageManager().get_postgres_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO task_metrics 
                    (task_id, start_time, end_time, duration, status, error)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (task_id, start_time, end_time, duration, status, error))
                conn.commit() 