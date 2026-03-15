# OpenWeatherAPI Data Engineering Pipeline

## Overview
This project demonstrates an end-to-end data engineering pipeline that ingests weather data from the OpenWeather API, processes it, stores it in PostgreSQL, transforms it using dbt, and visualizes insights using Apache Superset.

The pipeline is orchestrated with Apache Airflow and containerized using Docker to simulate a production-like data platform.

## Architecture

![Architecture](openweatherapi_architecture.png)

1. Extract weather data from OpenWeather API using Python

2. Load raw data into PostgreSQL

3. Transform and model the data using dbt

4. Orchestrate tasks with Apache Airflow

5. Visualize insights using Apache Superset

6. Run services inside Docker containers

## Tech Stack

- Python – Data extraction
- PostgreSQL – Data storage
- dbt – Data transformation & modeling
- Apache Airflow – Pipeline orchestration
- Apache Superset – Data visualization
- Docker – Containerization

## Pipeline Flow

1. Extract weather data using Python from OpenWeather API
2. Load raw data into PostgreSQL
3. Transform and model data using dbt
4. Orchestrate the pipeline using Airflow
5. Visualize data using Superset dashboards

## Project Structure
OpenWeatherAPI/
dags/                         # Apache Airflow DAGs for orchestration
logs/                         # Airflow logs
openweather_dbt/              # dbt project (models, tests, transformations)
plugins/                      # Airflow custom plugins
screenshots/
ETL/                          # Code or terminal screenshots
dbt/                          # Models, tests, terminal output
PostgreSQL/                   # pgAdmin / query output
Superset/                     # Charts and dashboards
superset/                     # Apache Superset Docker configuration
venv/                         # Python virtual environment

docker-compose.yml            # Docker services configuration
openweatherapi_architecture.png # Pipeline architecture diagram
requirements.txt              # Python dependencies
README.md                     # Project documentation

## How run the project

1. Clone the repo
git clone https://github.com/yourusername/openweather-data-engineering-pipeline.git
cd openweather-data-engineering-pipeline
2. Create a virtual environment
python3 -m venv venv
source venv/bin/activate
3. Install dependencies
pip install -r requirements.txt
4. Start services using Docker
docker-compose up -d
5. cd superset
docker-compose -f docker-compose-image-tag.yml up -d
cd .. (to go back to OpenWeatherAPI)
6. Run dbt models
cd ../openweather_dbt
dbt run
dbt test
7. Access Superset (open your browser)
http://localhost:8088

## Features

- Automated ETL pipeline

- Data transformation using dbt

- Data validation tests

- Workflow orchestration with Airflow

- Interactive dashboards with Superset

- Dockerized environment

## Author

Mnelisi Masilela
BSc IT Graduate | Data Engineering Portfolio Project
