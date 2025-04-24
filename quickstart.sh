#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e
# Treat unset variables as an error when substituting.
set -u
# Pipelines return the exit status of the last command to exit with a non-zero status,
# or zero if no command exited with a non-zero status.
set -o pipefail

# --- Configuration ---
# Optional: Define service names if they differ from standard conventions
REDIS_SERVICE="redis"
MINIO_SERVICE="minio"
ZOOKEEPER_SERVICE="zookeeper"
KAFKA_SERVICE="kafka"
JOBMANAGER_SERVICE="jobmanager"
# Add taskmanager service(s) if you need to wait for them specifically
# TASKMANAGER_SERVICE="taskmanager"

# --- Helper Functions ---
log() {
  echo "--------------------------------------------------"
  echo "$@"
  echo "--------------------------------------------------"
}

# Function to wait for Docker Compose services to be 'running'
# Usage: wait_for_services service1 [service2 ...]
wait_for_services() {
  local services=("$@")
  local max_wait_seconds=60 # Adjust timeout as needed (increased default)
  local interval_seconds=5
  local elapsed_seconds=0
  local all_running=0

  log "Waiting for services to be running: ${services[*]}"

  while [ $elapsed_seconds -lt $max_wait_seconds ]; do
    all_running=1 # Assume all are running initially
    for service in "${services[@]}"; do
      # Check if the service is listed and its status is 'running'
      # We removed the 'healthy' filter as it requires explicit healthchecks
      # in the compose file, which might not be present. 'running' is sufficient
      # for basic readiness after 'up -d'.
      local service_check_output
      # Capture output and exit code separately for debugging if needed
      service_check_output=$(docker compose ps --filter "status=running" -q "$service" 2>&1)
      local exit_code=$?

      # Check if the command succeeded AND produced output (a container ID)
      # grep -q . checks for any character (non-empty output)
      if [ $exit_code -ne 0 ] || ! echo "$service_check_output" | grep -q .; then
        # echo "DEBUG: Service '$service' not ready yet. Exit code: $exit_code, Output: [$service_check_output]" # Uncomment for debugging
        all_running=0 # Found a service not running yet
        break # No need to check other services in this iteration
      fi
    done

    if [ $all_running -eq 1 ]; then
      log "All requested services (${services[*]}) are running."
      return 0 # Success
    fi

    sleep $interval_seconds
    elapsed_seconds=$((elapsed_seconds + interval_seconds))
    echo "Waiting... (${elapsed_seconds}s / ${max_wait_seconds}s)"
  done

  log "ERROR: Timeout waiting for services (${services[*]}) after ${max_wait_seconds} seconds."
  docker compose ps # Show status on failure
  return 1 # Failure
}


# --- Main Script Logic ---

# 1. Clean up previous environment
log "Stopping and removing existing Docker containers and volumes..."
docker compose down -v --remove-orphans # Added --remove-orphans for extra cleanup

# 2. Start Core Infrastructure (Redis, Minio)
log "Starting core infrastructure: $REDIS_SERVICE, $MINIO_SERVICE..."
docker compose up -d "$REDIS_SERVICE" "$MINIO_SERVICE"
wait_for_services "$REDIS_SERVICE" "$MINIO_SERVICE"

# 3. Run Formatting (Assuming this prepares something needed by Flink/Kafka)
log "Running formatting script..."
if [ -f ./format.sh ]; then
  ./format.sh
else
  log "WARNING: ./format.sh not found. Skipping."
fi

# 4. Start Flink Cluster Components
log "Starting Flink cluster components: $ZOOKEEPER_SERVICE, $KAFKA_SERVICE, $JOBMANAGER_SERVICE, taskmanager(s)..."
# Assuming 'taskmanager' is the service name pattern in docker-compose.yml
docker compose up -d "$ZOOKEEPER_SERVICE" "$KAFKA_SERVICE" "$JOBMANAGER_SERVICE" taskmanager # Use the actual service name(s)
# Waiting for Flink cluster readiness is complex.
# A simple check is to wait for the JobManager. TaskManagers might take longer to register.
# Using a longer sleep is a pragmatic compromise for a quickstart.
# For true robustness, you'd query the Flink REST API.
wait_for_services "$ZOOKEEPER_SERVICE" "$KAFKA_SERVICE" "$JOBMANAGER_SERVICE" # Add taskmanager service if needed
log "Waiting for Flink cluster to stabilize (using fixed sleep)..."
sleep 20 # Increased sleep, add comment about why

# 5. Submit Flink Job
log "Submitting Flink job..."
if [ -f ./manage-flink-job.sh ]; then
  ./manage-flink-job.sh \
    -m com.sjsu.flink.KafkaToJuice \
    -n "Kafka To Juice Operations on network_rail" \
    -d
else
  log "WARNING: ./manage-flink-job.sh not found. Skipping Flink job submission."
fi

# 6. Set up and Run Python Producer
log "Setting up and running Python producer..."
PRODUCER_DIR="producer_src"
VENV_DIR="$PRODUCER_DIR/venv"
REQ_FILE="$PRODUCER_DIR/requirements.txt"
PRODUCER_SCRIPT="$PRODUCER_DIR/opendata-nationalrail-client.py"

if [ ! -d "$PRODUCER_DIR" ]; then
    log "ERROR: Producer directory '$PRODUCER_DIR' not found."
    exit 1
fi

cd "$PRODUCER_DIR"

# Create venv only if it doesn't exist
if [ ! -d "venv" ]; then
  log "Creating Python virtual environment in '$VENV_DIR'..."
  python3 -m venv venv
else
  log "Python virtual environment '$VENV_DIR' already exists. Skipping creation."
fi

# Check if requirements file exists
if [ ! -f "requirements.txt" ]; then
    log "ERROR: Requirements file 'requirements.txt' not found in '$PRODUCER_DIR'."
    cd .. # Go back to original directory before exiting
    exit 1
fi

# Install dependencies using the venv's pip
log "Installing dependencies from 'requirements.txt' using venv pip..."
# Use the specific pip from the venv
"venv/bin/pip" install -r requirements.txt

# Check if producer script exists
if [ ! -f "opendata-nationalrail-client.py" ]; then
    log "ERROR: Producer script 'opendata-nationalrail-client.py' not found in '$PRODUCER_DIR'."
    cd .. # Go back to original directory before exiting
    exit 1
fi

# Run the producer script using the venv's python
log "Running producer script 'opendata-nationalrail-client.py' using venv python..."
# Use the specific python from the venv
"venv/bin/python" opendata-nationalrail-client.py

# Optional: Go back to the original directory if needed by subsequent steps
cd ..

log "Quickstart script finished."

exit 0