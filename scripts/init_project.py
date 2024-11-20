import os
import sys
import subprocess
import logging
from pathlib import Path
import shutil

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ProjectInitializer:
    def __init__(self):
        self.project_root = Path(__file__).parent.parent
        self.required_dirs = [
            'dags',
            'logs',
            'plugins',
            'data',
            'src/config',
            'src/utils',
            'src/tasks/news',
            'src/tasks/movies',
            'tests'
        ]

    def init_project(self):
        """Initialize the complete project structure"""
        try:
            self._create_directory_structure()
            self._create_env_file()
            self._verify_docker_installation()
            self._install_requirements()
            self._setup_permissions()
            self._init_git()
            return True
        except Exception as e:
            logger.error(f"Project initialization failed: {e}")
            return False

    def _create_directory_structure(self):
        """Create project directory structure"""
        for dir_path in self.required_dirs:
            full_path = self.project_root / dir_path
            full_path.mkdir(parents=True, exist_ok=True)
            (full_path / '__init__.py').touch()
            logger.info(f"Created directory: {dir_path}")

    def _verify_docker_installation(self):
        """Verify Docker and Docker Compose installation"""
        try:
            subprocess.run(['docker', '--version'], check=True, capture_output=True)
            subprocess.run(['docker-compose', '--version'], check=True, capture_output=True)
            logger.info("Docker and Docker Compose are installed")
        except subprocess.CalledProcessError as e:
            raise Exception(f"Docker verification failed: {str(e)}")

    def _create_env_file(self):
        """Create .env file if it doesn't exist"""
        env_path = self.project_root / '.env'
        if not env_path.exists():
            env_content = """
AIRFLOW_UID=50000
AIRFLOW_GID=50000
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__LOAD_EXAMPLES=false
AIRFLOW__CORE__ENABLE_XCOM_PICKLING=true

POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
POSTGRES_HOST=postgres
POSTGRES_PORT=5432

MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_HOST=minio:9000
            """.strip()
            env_path.write_text(env_content)
            logger.info("Created .env file")

    def _install_requirements(self):
        """Create requirements.txt file"""
        requirements = [
            'apache-airflow==2.8.1',
            'pyspark==3.5.0',
            'requests==2.31.0',
            'beautifulsoup4==4.12.3',
            'psycopg2-binary==2.9.9',
            'sqlalchemy==1.4.51',
            'python-dotenv==1.0.1',
            'minio==7.2.3',
            'pandas>=2.0.0',
            'pytest==8.0.1',
            'pytest-mock==3.12.0',
            'wget==3.2',
            'psutil==5.9.8',
            'pendulum==2.1.2',
            'py4j==0.10.9.7',
            'numpy>=1.24.0',
            'pyppeteer==1.0.2',
            'websockets==10.3',
            'nltk==3.8.1'
        ]
        
        req_path = self.project_root / 'requirements.txt'
        req_path.write_text('\n'.join(requirements))
        logger.info("Created requirements.txt file")

    def _setup_permissions(self):
        """Setup correct permissions for Airflow"""
        try:
            for dir_name in ['logs', 'data']:
                dir_path = self.project_root / dir_name
                os.chmod(dir_path, 0o777)
            logger.info("Set directory permissions")
        except Exception as e:
            raise Exception(f"Failed to set permissions: {e}")

    def _init_git(self):
        """Initialize git repository if not already initialized"""
        git_dir = self.project_root / '.git'
        if not git_dir.exists():
            try:
                subprocess.run(['git', 'init'], cwd=self.project_root, check=True)
                gitignore_content = """
# Python
__pycache__/
*.py[cod]
*.so
.Python
env/
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
*.egg-info/
.installed.cfg
*.egg

# Airflow
logs/
airflow.db
airflow.cfg
unittests.cfg
webserver_config.py

# Environment
.env
.venv/
venv/
ENV/

# Data
data/
*.csv
*.json

# IDE
.idea/
.vscode/
*.swp
*.swo
                """.strip()
                gitignore_path = self.project_root / '.gitignore'
                gitignore_path.write_text(gitignore_content)
                logger.info("Initialized git repository")
            except subprocess.CalledProcessError:
                logger.warning("Git not installed, skipping git initialization")

if __name__ == "__main__":
    initializer = ProjectInitializer()
    if initializer.init_project():
        logger.info("Project initialized successfully!")
    else:
        logger.error("Project initialization failed!")
        sys.exit(1) 