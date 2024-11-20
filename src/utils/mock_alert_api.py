import logging
from datetime import datetime

class MockAlertAPI:
    @staticmethod
    def send_alert(context):
        """
        Mock API to send alerts for pipeline failures
        Simulates an external alerting service
        """
        task = context.get('task_instance')
        dag_id = task.dag_id
        task_id = task.task_id
        execution_date = context.get('execution_date')
        error = context.get('exception')
        
        alert_message = f"""
        🚨 Pipeline Alert
        Time: {datetime.now()}
        Pipeline: {dag_id}
        Failed Task: {task_id}
        Execution Date: {execution_date}
        Error: {str(error)}
        """
        
        # Simulate API call
        logging.error(f"MOCK ALERT API CALLED:\n{alert_message}")
        return {"status": "sent", "timestamp": datetime.now().isoformat()} 