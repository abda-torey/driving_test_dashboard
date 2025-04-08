#!/bin/bash

# Set environment variables if needed (adjust AIRFLOW_UID if required)
AIRFLOW_UID=1001:0  # Default to 1001 if AIRFLOW_UID is not set (container UID:GID)

# Local user (to match the UID of the files on the host)
LOCAL_USER_UID=$(id -u gedi5685)  # Replace gedi5685 with your local username if necessary
LOCAL_USER_GID=$(id -g gedi5685)  # Replace gedi5685 with your local username if necessary

# Directories to check and fix
DIRECTORIES=("./dags" "./logs" "./plugins" "./keys" "./src" "./data")

# Function to fix permissions
fix_permissions() {
  for dir in "${DIRECTORIES[@]}"; do
    if [ ! -d "$dir" ]; then
      echo "Creating directory: $dir"
      mkdir -p "$dir"
    fi

    # Fix the permissions for local file system (matching local user)
    echo "Setting ownership and permissions for $dir on local system"
    sudo chown -R "$LOCAL_USER_UID:$LOCAL_USER_GID" "$dir"
    sudo chmod -R 775 "$dir"

    # Fix permissions for container (match AIRFLOW_UID for container)
    echo "Setting ownership and permissions for $dir inside container"
    sudo chown -R "$AIRFLOW_UID" "$dir"
    sudo chmod -R 775 "$dir"
  done
}

# Function to check if Docker is running and restart it
restart_docker_containers() {
  echo "Restarting Docker containers..."
  docker compose down
  docker compose up -d
}

# Run the permission fix function
fix_permissions

# Restart Docker containers
restart_docker_containers

echo "Permission fix and Docker restart completed!"
