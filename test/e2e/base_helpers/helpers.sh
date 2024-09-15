# test/e2e/base_helpers/helpers.sh

# Function to print debug logs when the debug mode is enabled
print_debug() {
  local DEBUG=$1
  shift
  if [ "$DEBUG" = true ]; then
    echo "[DEBUG] $*"
  fi
}

# Function to ensure BASE_DIR is provided as a mandatory parameter
ensure_base_dir_provided() {
  if [ -z "$BASE_DIR" ]; then
    echo "Error: BASE_DIR is required as a mandatory parameter."
    exit 1
  fi
}



# Function to generate a self-signed certificate and key (requires BASE_DIR)
generate_tls_cert() {
  local BASE_DIR=$1
  local DEBUG=$2
  local CERT_SUBDIR="nginx_configs"
  
  # Define the directory where the certificates will be stored
  local CERT_DIR="$BASE_DIR/$CERT_SUBDIR"
  
  # Check if the nginx_configs directory exists
  if [ -d "$CERT_DIR" ]; then
    print_debug "$DEBUG" "Generating TLS certificate and key in $CERT_DIR..."
    
    # Generate the self-signed certificate and key
    openssl req -x509 -nodes -days 1 -newkey rsa:2048 \
      -keyout "$CERT_DIR/nginx.key" \
      -out "$CERT_DIR/nginx.crt" \
      -subj "/C=US/ST=State/L=City/O=Organization/OU=Unit/CN=localhost" > /dev/null 2>&1
    
    # Ensure the permissions are set correctly
    chmod 644 "$CERT_DIR/nginx."*
    
    print_debug "$DEBUG" "TLS certificate and key have been successfully generated in $CERT_DIR."
  else
    print_debug "$DEBUG" "Directory $CERT_DIR does not exist. Skipping TLS certificate generation as it is not needed for this test."
  fi
}

# Function to start Docker Compose infrastructure (requires BASE_DIR)
start_infrastructure() {
  local BASE_DIR=$1
  local DEBUG=$2
  echo "Starting infrastructure..."
  if [ "$DEBUG" = true ]; then
    docker-compose -f "$BASE_DIR/docker/docker-compose.yml" up -d
  else
    docker-compose -f "$BASE_DIR/docker/docker-compose.yml" up -d > /dev/null 2>&1
  fi
}

# Function to stop Docker Compose infrastructure
stop_infrastructure() {
  echo "Stopping infrastructure..."
  if [ "$DEBUG" = true ]; then
    docker-compose -f "$BASE_DIR/docker/docker-compose.yml" down --volumes
  else
    docker-compose -f "$BASE_DIR/docker/docker-compose.yml" down --volumes > /dev/null 2>&1
  fi
}

# Function to build and run the Kafka application
run_kafka_lag_app() {
  local config_file=$1
  local DEBUG=$2
  print_debug "$DEBUG" "Checking go.mod..."
  if [ ! -f go.mod ]; then
    go mod init github.com/sciclon2/kafka-lag-go
    go mod edit -go=1.20
  fi
  go mod tidy

  echo "Starting kafka-lag-go application with config file: $config_file"
  if [ "$DEBUG" = true ]; then
    go build -o bin/kafka-lag-go cmd/kafka-lag-go/main.go
    ./bin/kafka-lag-go --config-file "$config_file" &
  else
    go build -o bin/kafka-lag-go cmd/kafka-lag-go/main.go > /dev/null 2>&1
    ./bin/kafka-lag-go --config-file "$config_file" > /dev/null 2>&1 &
  fi
  APP_PID=$!
}


# Function to wait for the health check to pass
wait_for_health_check() {
  echo "Waiting for the application health check to pass..."
  for i in $(seq 1 10); do
    local http_status=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/healthz || echo "fail")
    print_debug "$DEBUG" "Health check status: $http_status"
    if [[ "$http_status" =~ ^[0-9]+$ ]] && [ "$http_status" -eq 200 ]; then
      print_debug "$DEBUG" "Application is healthy."
      break
    else
      print_debug "$DEBUG" "Health check failed, retrying in 1 second..."
      sleep 1
    fi
    if [ "$i" -eq 10 ]; then
      echo "Application failed to pass health check after 10 seconds."
      exit 1
    fi
  done
}


# Function to send SIGUSR1 signals to force new iterations
send_signals() {
  echo "Sending SIGUSR1 signals to force new iterations in kafka-lag-go..."
  for i in $(seq 1 7); do
    kill -USR1 $APP_PID
    print_debug "$DEBUG" "Sent SIGUSR1 signal $i"
    sleep 1
  done
}


# Function to run end-to-end tests (requires BASE_DIR)
run_tests() {
  local BASE_DIR=$1
  local DEBUG=$2
  local FOLDER_NAME=$(basename "$BASE_DIR")
  echo "Running end-to-end tests: $FOLDER_NAME"
  RUN_E2E_TESTS=true go test "./test/e2e/tests/$FOLDER_NAME/..." -cover -count=1 -v
}

# Cleanup function to stop the application and Docker Compose services
cleanup() {
  print_debug "$DEBUG" "Stopping the application..."
  if [ -n "$APP_PID" ] && kill -0 "$APP_PID" 2>/dev/null; then
    kill "$APP_PID"
  fi
  stop_infrastructure "$BASE_DIR" "$DEBUG"
}

# Function for sleep with echo
wait_for_processing() {
  echo "Ensuring some records are produced and consumed in Kafka, sleeping for 10 seconds..."
  sleep 10
}