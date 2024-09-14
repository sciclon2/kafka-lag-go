#!/bin/bash

set -e

# Check if the debug flag is set (use -d or --debug to enable debug mode)
DEBUG=false
for arg in "$@"; do
  case $arg in
    -d|--debug)
      DEBUG=true
      shift
      ;;
    *)
      shift
      ;;
  esac
done

# Function to print debug logs when the debug mode is enabled
debug() {
  if [ "$DEBUG" = true ]; then
    echo "[DEBUG] $*"
  fi
}

# Directory where certificates will be stored
CERT_DIR="./test/e2e/nginx_configs"

# Function to generate a self-signed certificate and key
generate_tls_cert() {
  debug "Generating TLS certificate and key..."
  mkdir -p $CERT_DIR
  openssl req -x509 -nodes -days 1 -newkey rsa:2048 \
    -keyout $CERT_DIR/nginx.key \
    -out $CERT_DIR/nginx.crt \
    -subj "/C=US/ST=State/L=City/O=Organization/OU=Unit/CN=localhost" > /dev/null 2>&1
  chmod 644 $CERT_DIR/nginx.*
  debug "TLS certificate and key have been successfully generated."
}

# Function to start Docker Compose infrastructure
start_infrastructure() {
  echo "Starting infrastructure..."
  if [ "$DEBUG" = true ]; then
    docker-compose -f test/e2e/docker/docker-compose.yml up -d
  else
    docker-compose -f test/e2e/docker/docker-compose.yml up -d > /dev/null 2>&1
  fi
}

# Function to build and run the Kafka application
run_kafka_app() {
  debug "Checking go.mod..."
  if [ ! -f go.mod ]; then
    go mod init github.com/sciclon2/kafka-lag-go
    go mod edit -go=1.20 
  fi
  go mod tidy

  echo "Starting kafka-lag-go application..."
  if [ "$DEBUG" = true ]; then
    go build -o bin/kafka-lag-go cmd/kafka-lag-go/main.go
    ./bin/kafka-lag-go --config-file test/e2e/kafka-lag-go-configs/config_remote_write_basic_auth.yaml &
  else
    go build -o bin/kafka-lag-go cmd/kafka-lag-go/main.go > /dev/null 2>&1
    ./bin/kafka-lag-go --config-file test/e2e/kafka-lag-go-configs/config_remote_write_basic_auth.yaml > /dev/null 2>&1 &
  fi
  APP_PID=$!
}

# Function to wait for the application health check to pass
wait_for_health_check() {
  echo "Waiting for the application health check to pass..."
  for i in $(seq 1 10); do
    http_status=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/healthz || echo "fail")
    debug "Health check status: $http_status"
    if [ "$http_status" -eq 200 ]; then
      debug "Application is healthy."
      break
    else
      debug "Health check failed, retrying in 1 second..."
      sleep 1
    fi
    if [ "$i" -eq 10 ]; then
      echo "Application failed to pass health check after 10 seconds."
      exit 1
    fi
  done
}

# Function to send SIGUSR1 signals to the application
send_signals() {
  echo "Sending SIGUSR1 signals to force new iterations..."
  for i in $(seq 1 7); do
    kill -USR1 $APP_PID
    debug "Sent SIGUSR1 signal $i"
    sleep 1
  done
}

# Function to run end-to-end tests
run_tests() {
  echo "Running end-to-end tests..."
  RUN_E2E_TESTS=true go test ./test/e2e/... -cover -count=1 -v
}

# Function for sleep with echo
wait_for_processing() {
  echo "Ensuring some records are produced and consumed, sleeping for 10 seconds..."
  sleep 10
}

# Cleanup function to stop the application and Docker Compose services
cleanup() {
  echo "Stopping the application..."
  if [ -n "$APP_PID" ] && kill -0 "$APP_PID" 2>/dev/null; then
    kill "$APP_PID"
  fi
  echo "Stopping infrastructure..."
  if [ "$DEBUG" = true ]; then
    docker-compose -f test/e2e/docker/docker-compose.yml down --volumes
  else
  echo "A"
    #docker-compose -f test/e2e/docker/docker-compose.yml down --volumes > /dev/null 2>&1
  fi
}

# Set up a trap to call the cleanup function on exit
trap cleanup EXIT

# Main script execution
generate_tls_cert
start_infrastructure
wait_for_processing
run_kafka_app
wait_for_health_check
send_signals
run_tests

# Cleanup will be called automatically when the script exits