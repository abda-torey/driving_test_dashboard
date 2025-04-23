#!/bin/bash
# docker-entrypoint-fix.sh

# Add airflow user to docker group
groupadd -g 998 docker 2>/dev/null || true
usermod -aG docker airflow 2>/dev/null || true

# Execute the original entrypoint
exec /entrypoint "$@"
