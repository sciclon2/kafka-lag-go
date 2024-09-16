#!/bin/bash

set -e

# Source the helpers.sh library
source "$(dirname "$0")/base_helpers/helpers.sh"
export PATH=$PATH:~/.docker/cli-plugins/

# Check if the base directory is provided
BASE_DIR=$1
ensure_base_dir_provided

# Debug mode and only-start-infrastructure mode
DEBUG=false
ONLY_START_INFRASTRUCTURE=false
for arg in "$@"; do
  case $arg in
    -d|--debug)
      DEBUG=true
      shift
      ;;
    --only-start-infrastructure)
      ONLY_START_INFRASTRUCTURE=true
      shift
      ;;
    *)
      shift
      ;;
  esac
done

# Trap to handle any errors or script exits and perform cleanup
trap cleanup EXIT ERR

# Main script execution
generate_tls_cert "$BASE_DIR" "$DEBUG"
start_infrastructure "$BASE_DIR" "$DEBUG"

# If only starting infrastructure, skip specific steps but still run tests and stop infra
if [ "$ONLY_START_INFRASTRUCTURE" = true ]; then
  echo "Infrastructure started."
  run_tests "$BASE_DIR" "$DEBUG"
  exit 0
fi

# Continue with the rest if --only-start-infrastructure is not set
wait_for_processing
run_kafka_lag_app "$BASE_DIR/kafka-lag-go-configs/config.yaml" "$DEBUG"
wait_for_health_check
send_signals "$DEBUG"
run_tests "$BASE_DIR" "$DEBUG"

# cleanup will be executed by the trap and stop_infrastructure and/or stop the app