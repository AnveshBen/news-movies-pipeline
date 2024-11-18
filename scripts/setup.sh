#!/bin/bash

# setup.sh
echo "Setting up Airflow environment..."

# Create required directories
mkdir -p ./dags ./logs ./plugins

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    cat > .env << EOL
AIRFLOW_UID=$(id -u)
AIRFLOW_GID=0
EOL
fi

# Create requirements.txt if it doesn't exist
if [ ! -f requirements.txt ]; then
    cat > requirements.txt << EOL
apache-airflow==2.7.1
pandas
requests
beautifulsoup4
psycopg2-binary
sqlalchemy
python-dotenv
EOL
fi

# Build and start the containers
docker-compose up -d --build

# Wait for services to be ready
echo "Waiting for services to be ready..."
sleep 30

# Initialize Airflow
docker-compose exec airflow-webserver airflow db init

# Create default admin user
docker-compose exec airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

echo "Setup completed! Airflow is running at http://localhost:8080"
echo "Username: admin"
echo "Password: admin"