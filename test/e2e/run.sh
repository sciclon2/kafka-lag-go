#!/bin/bash

set -e

# Source the helpers.sh library
source "$(dirname "$0")/base_helpers/helpers.sh"
export PATH=$PATH:~/.docker/cli-plugins/

# Check if the base directory is provided
BASE_DIR=$1
ensure_base_dir_provided

# Debug mode
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

# Trap to handle any errors or script exits and perform cleanup
trap cleanup EXIT ERR

# Main script execution
generate_tls_cert "$BASE_DIR" "$DEBUG"
start_infrastructure "$BASE_DIR" "$DEBUG"
wait_for_processing
run_kafka_lag_app "$BASE_DIR/kafka-lag-go-configs/config.yaml" "$DEBUG"
wait_for_health_check
send_signals "$DEBUG"
run_tests "$BASE_DIR" "$DEBUG"
  
# Stop everything before the next iteration
stop_infrastructure "$BASE_DIR" "$DEBUG"
