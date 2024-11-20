# News & Movies Analysis Pipeline

An automated data pipeline system built with Apache Airflow that processes movie ratings and news sentiment analysis. Features two daily pipelines: news sentiment analysis (7 PM) and movie analysis (8 PM).

## System Requirements

- Docker and Docker Compose
- Python 3.9+
- 8GB RAM minimum
- Git

## Quick Setup

1. Clone the repository:

git clone <repository-url>

cd news-movies-pipeline

2. Run setup script:

chmod +x scripts/setup.sh
./scripts/setup.sh

3. Access services:
- Airflow UI: http://localhost:8080
- MinIO Console: http://localhost:9001

Default credentials:
- Airflow: admin/admin
- MinIO: minioadmin/minioadmin

## Pipeline Overview

### News Pipeline (7 PM Weekdays)
- Scrapes articles from YourStory and Finshots
- Processes HDFC and Tata Motors news (5 latest articles each)
- Performs sentiment analysis
- Stores results in PostgreSQL

### Movies Pipeline (8 PM Weekdays)
- Analyzes MovieLens 100K dataset
- Key analyses:
  - Mean age by occupation
  - Top 20 rated movies (35+ ratings)
  - Genre preferences by demographics
  - Movie similarity analysis

## Project Structure

.
├── dags/                  # Airflow DAG definitions
├── src/
│   ├── tasks/            # Pipeline operators
│   │   ├── movies/       # Movie analysis tasks
│   │   └── news/         # News processing tasks
│   ├── utils/            # Shared utilities
│   └── config/           # Configuration
├── tests/                # Test suite
└── scripts/              # Setup scripts

## Configuration

1. Environment variables are automatically set up in `.env`
2. Key settings in `src/config/settings.py`:



## Monitoring

1. Access Airflow UI for:
   - Task status
   - Logs
   - Pipeline dependencies

2. Health checks:
   - Service availability
   - Database connections
   - Storage access

## Troubleshooting

1. If setup fails:
   - Check Docker status
   - Verify port availability (8080, 5432, 9000, 9001)
   - Review logs: `docker-compose logs`

2. For pipeline issues:
   - Check Airflow logs
   - Verify MinIO connectivity
   - Ensure PostgreSQL is running



