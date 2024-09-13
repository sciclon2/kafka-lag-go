#!/bin/bash

#set -e

# Define the cleanup function to stop the application
cleanup() {
    echo "Stopping the application..."
    
    # Stop the application if it was started and still running
    if [ -n "$APP_PID" ] && kill -0 "$APP_PID" 2>/dev/null; then
        kill "$APP_PID"
    fi

    # Stop the Docker Compose services
    docker-compose -f test/e2e/setup/docker-compose.yml down --volumes
}

# Set up a trap to call the cleanup function on exit
trap cleanup EXIT

# Start the infrastructure using Docker Compose
echo "Starting infrastructure..."
docker-compose -f test/e2e/setup/docker-compose.yml up -d

echo "Ensuring some records are produced and consumed, sleeping for 10 seconds..."
sleep 10

# Ensure go.mod exists with correct Go version
echo "Checking go.mod..."
if [ ! -f go.mod ]; then
    go mod init github.com/sciclon2/kafka-lag-go
    go mod edit -go=1.20  # Set the Go version here
fi
go mod tidy

# Run the main application in the background
echo "Starting kafka-lag-go application..."
go build -o bin/kafka-lag-go cmd/kafka-lag-go/main.go
./bin/kafka-lag-go --config-file test/e2e/config.yaml &

# Capture the PID of the application
APP_PID=$!

# Wait for the application to become healthy (max wait time: 10 seconds)
echo "Waiting for the application health check to pass..."
for i in $(seq 1 10); do
    echo "Health check status: $status"  # Add this line for debugging
    http_status=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/healthz)
    if [ "$http_status" -eq 200 ]; then
        echo "Application is healthy."
        break
    else
        echo "Health check failed, retrying in 1 second..."
        sleep 1
    fi

    if [ "$i" -eq 10 ]; then
        echo "Application failed to pass health check after 10 seconds."
        exit 1
    fi
done



# Send SIGUSR1 signal 5 times, every 1 second
for i in $(seq 1 7); do
    kill -USR1 $APP_PID
    echo "Sent SIGUSR1 signal $i"
    sleep 1
done



# Run the end-to-end tests
echo "Running end-to-end tests..."
RUN_E2E_TESTS=true go test ./test/e2e/... -cover -count=1 -v

# The cleanup function will be called automatically when the script exits
