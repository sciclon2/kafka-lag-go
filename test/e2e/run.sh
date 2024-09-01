#!/bin/bash

set -e

# Define the cleanup function to stop the application
cleanup() {
    echo "Stopping the application..."
    # Add the command to stop your application here
    # For example, if you ran the app in the background using '&':
    kill $APP_PID

    # Stop the Docker Compose services
    docker-compose -f test/e2e/setup/docker-compose.yml down
}

# Set up a trap to call the cleanup function on exit
trap cleanup EXIT

# Start the infrastructure using Docker Compose
echo "Starting infrastructure..."
docker-compose -f test/e2e/setup/docker-compose.yml up -d

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
./bin/kafka-lag-go --config-file configs/config.yaml &

# Capture the PID of the application
APP_PID=$!

# Wait for the application to start up
sleep 2

# Run the end-to-end tests
echo "Running end-to-end tests..."
go test -v -tags=e2e ./test/e2e/...

# The cleanup function will be called automatically when the script exits
