#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e
# Print commands and their arguments as they are executed.
set -x

# Define variables
AIRFLOW_VERSION=2.7.0
PYTHON_VERSION=3.9
# Set a consistent UID and GID for Airflow, matching the current user's.
export AIRFLOW_UID=$(id -u)
export AIRFLOW_GID=0 #$(id -g)
export AIRFLOW_HOME=$(pwd)

# Create necessary directories for Airflow and monitoring if they don't exist
declare -a DIRS=(
  "./dags"
  "./logs"
  "./plugins"
  "./great_expectations"
  "./prometheus"
  "./grafana/provisioning/datasources"
  "./grafana/provisioning/dashboards"
)

for dir in "${DIRS[@]}"; do
  if [ ! -d "$dir" ]; then
    echo "Creating directory: $dir"
    mkdir -p "$dir"
  fi
done

# Set permissions for Airflow directories
# Use the current user's UID and GID to ensure proper volume access.
echo "Setting permissions for Airflow directories..."
chown -R ${AIRFLOW_UID}:${AIRFLOW_GID} ./dags ./logs ./plugins ./great_expectations ./data

# Start core services (db and redis) and build Airflow images
echo "Starting core services and building Airflow images..."
docker-compose up --build -d db redis
docker-compose build airflow-webserver airflow-scheduler airflow-worker

# Wait for PostgreSQL to be healthy
echo "Waiting for PostgreSQL database to be healthy..."
while ! docker-compose exec db pg_isready -U postgres >/dev/null 2>&1; do
  echo -n "."
  sleep 1
done
echo "PostgreSQL is ready."

# Initialize Airflow database (idempotent)
echo "Initializing Airflow database..."
docker-compose run --rm airflow-webserver bash -c '
  export AIRFLOW_HOME=/opt/airflow && 
  airflow db migrate
'

# Create Airflow admin user (idempotent)
echo "Creating or updating Airflow admin user..."
docker-compose run --rm airflow-webserver bash -c '
  export AIRFLOW_HOME=/opt/airflow && 
  airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin || echo "Admin user may already exist."
'

# Start all services with force-recreate to ensure fresh containers
echo "Starting all services..."
docker-compose up -d --force-recreate

# Wait for Airflow webserver to be ready with a health check
echo "Waiting for Airflow webserver to be ready..."
ATTEMPTS=0
MAX_ATTEMPTS=120  # Reduced wait time to 2 minutes
while [ $ATTEMPTS -lt $MAX_ATTEMPTS ]; do
  if curl -sSf http://localhost:8081/health >/dev/null 2>&1; then
    echo "Airflow webserver is ready."
    break
  fi
  echo -n "."
  sleep 1
  ATTEMPTS=$((ATTEMPTS+1))
done

if [ $ATTEMPTS -eq $MAX_ATTEMPTS ]; then
  echo "Airflow webserver health check failed after multiple attempts."
  echo "Checking logs for airflow-webserver..."
  docker-compose logs --tail=50 airflow-webserver
  exit 1
fi

echo "Airflow setup complete. Access UIs:"
echo "  - Airflow:   http://localhost:8081 (user: admin, pass: admin)"
echo "  - Prometheus: http://localhost:9090"
echo "  - Grafana:    http://localhost:3000 (user: admin, pass: admin)"