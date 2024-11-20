from src.utils.mock_alert_api import MockAlertAPI

def task_failure_callback(context):
    """Callback function for task failures"""
    MockAlertAPI.send_alert(context)

def dag_failure_callback(context):
    """Callback function for DAG failures"""
    MockAlertAPI.send_alert(context)

def sla_miss_callback(context):
    """Callback function for SLA misses"""
    MockAlertAPI.send_alert(context) 