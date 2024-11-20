import os
import psutil
import logging
from typing import Dict, Any

class Troubleshooter:
    @staticmethod
    def check_system_resources() -> Dict[str, Any]:
        """Check system resources"""
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        return {
            'memory_available': f"{memory.available / (1024 * 1024 * 1024):.2f}GB",
            'disk_available': f"{disk.free / (1024 * 1024 * 1024):.2f}GB",
            'cpu_usage': f"{psutil.cpu_percent()}%"
        }

    @staticmethod
    def verify_permissions():
        """Verify directory permissions"""
        dirs_to_check = [
            '/opt/airflow/dags',
            '/opt/airflow/logs',
            '/opt/airflow/data'
        ]
        
        issues = []
        for dir_path in dirs_to_check:
            if not os.access(dir_path, os.W_OK):
                issues.append(f"No write permission: {dir_path}")
        return issues

    @staticmethod
    def check_connections():
        """Check all service connections"""
        from src.utils.health_check import HealthCheck
        return HealthCheck.check_all_services() 