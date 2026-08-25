#!/bin/bash

# Load environment variables from .env file if it exists to get the correct PORT
if [ -f .env ]; then
  # Sourcing .env safely without breaking on comments or spaces
  export $(grep -v '^#' .env | xargs)
fi

# Use the PORT from .env, or fallback to 3000
SERVICE_PORT=${PORT:-3000}

clear
echo "=========================================================="
echo "          MongoDB Sync Service Monitor CLI               "
echo "=========================================================="
echo "Select a monitoring option:"
echo "1) Tail live container console logs (stdout/stderr)"
echo "2) Tail Daily combined log file (inside Docker volume)"
echo "3) Tail Daily error log file (inside Docker volume)"
echo "4) Check Service Health API (JSON status)"
echo "5) View Live Sync metrics (Prometheus format)"
echo "6) Exit"
echo "=========================================================="
read -p "Enter choice [1-6]: " choice

case $choice in
  1)
    echo "Tailing live container console logs (Press Ctrl+C to stop)..."
    docker compose logs -f sync
    ;;
  2)
    DATE_STR=$(date +%Y-%m-%d)
    echo "Tailing daily combined log file for $DATE_STR (Press Ctrl+C to stop)..."
    docker compose exec sync sh -c "tail -f /app/logs/combined/$DATE_STR.log 2>/dev/null || echo 'No log file found yet for today ($DATE_STR).'"
    ;;
  3)
    DATE_STR=$(date +%Y-%m-%d)
    echo "Tailing daily error log file for $DATE_STR (Press Ctrl+C to stop)..."
    docker compose exec sync sh -c "tail -f /app/logs/errors/$DATE_STR.log 2>/dev/null || echo 'No error log file found for today ($DATE_STR).'"
    ;;
  4)
    echo "Fetching service health from http://localhost:$SERVICE_PORT/health..."
    if command -v jq &> /dev/null; then
      curl -s http://localhost:$SERVICE_PORT/health | jq .
    else
      curl -s http://localhost:$SERVICE_PORT/health
      echo -e "\nNote: Install 'jq' on your host for formatted JSON output."
    fi
    ;;
  5)
    echo "Fetching live metrics from http://localhost:$SERVICE_PORT/metrics..."
    curl -s http://localhost:$SERVICE_PORT/metrics
    ;;
  6)
    echo "Exiting."
    exit 0
    ;;
  *)
    echo "Invalid option. Exiting."
    exit 1
    ;;
esac
