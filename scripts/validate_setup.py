import sys
import logging
from pathlib import Path
import subprocess
import psutil
import requests
from minio import Minio
import psycopg2
from pyspark.sql import SparkSession
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SetupValidator:
    def __init__(self):
        self.project_root = Path(__file__).parent.parent

    def validate_all(self):
        """Run all validation checks"""
        checks = [
            self._check_system_resources,
            self._check_docker_services,
            self._check_airflow_connection,
            self._check_postgres_connection,
            self._check_minio_connection,
            self._check_spark_setup,
            self._check_file_permissions
        ]
        
        issues = []
        for check in checks:
            try:
                check()
                logger.info(f"✅ {check.__name__} passed")
            except Exception as e:
                issues.append(f"❌ {check.__name__} failed: {str(e)}")
                logger.error(f"{check.__name__} failed: {str(e)}")
        
        return issues

    def _check_system_resources(self):
        """Verify system has sufficient resources"""
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        if memory.available < 4 * 1024 * 1024 * 1024:  # 4GB
            raise Exception("Insufficient memory: need at least 4GB free")
            
        if disk.free < 10 * 1024 * 1024 * 1024:  # 10GB
            raise Exception("Insufficient disk space: need at least 10GB free")

    def _check_docker_services(self):
        """Verify all required Docker services are running"""
        try:
            # Use docker ps command to check running containers
            result = subprocess.run(
                ['docker', 'ps', '--format', '{{.Names}}'],
                capture_output=True,
                text=True,
                check=True
            )
            running_containers = result.stdout.split('\n')
            
            required_services = ['airflow-webserver', 'postgres', 'minio']
            for service in required_services:
                if not any(service in container for container in running_containers):
                    raise Exception(f"Service {service} is not running")
                    
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to check Docker services: {str(e)}")

    def _check_airflow_connection(self):
        """Verify Airflow web server is accessible"""
        try:
            response = requests.get('http://localhost:8080/health')
            if response.status_code != 200:
                raise Exception("Airflow web server is not responding")
        except requests.exceptions.ConnectionError:
            raise Exception("Could not connect to Airflow web server")

    def _check_postgres_connection(self):
        """Verify PostgreSQL connection"""
        try:
            conn = psycopg2.connect(
                host="localhost",
                port=5432,
                database="airflow",
                user="airflow",
                password="airflow"
            )
            conn.close()
        except Exception as e:
            raise Exception(f"PostgreSQL connection failed: {str(e)}")

    def _check_minio_connection(self):
        """Verify MinIO connection"""
        try:
            minio_client = Minio(
                "localhost:9000",
                access_key="minioadmin",
                secret_key="minioadmin",
                secure=False
            )
            minio_client.list_buckets()
        except Exception as e:
            raise Exception(f"MinIO connection failed: {str(e)}")

    def _check_spark_setup(self):
        """Verify Spark setup"""
        try:
            spark = SparkSession.builder.appName("test").getOrCreate()
            spark.stop()
        except Exception as e:
            raise Exception(f"Spark setup failed: {str(e)}")

    def _check_file_permissions(self):
        """Verify file permissions"""
        dirs_to_check = ['logs', 'data']
        for dir_name in dirs_to_check:
            dir_path = self.project_root / dir_name
            if not os.access(dir_path, os.W_OK):
                raise Exception(f"No write permission for {dir_name} directory")

if __name__ == "__main__":
    validator = SetupValidator()
    issues = validator.validate_all()
    
    if issues:
        logger.error("Setup validation failed:")
        for issue in issues:
            logger.error(issue)
        sys.exit(1)
    else:
        logger.info("All validations passed successfully!") 