## Table of Contents

1. [Kafka Lag Go](#kafka-lag-go)
2. [Features](#features)
3. [Architecture Overview](#architecture-overview)
4. [Performance](#performance)
    1. [Prerequisites](#prerequisites)
    2. [Platform Compatibility](#platform-compatibility)
5. [Installation](#installation)
6. [Deployment Options](#deployment-options)
    1. [Step 1: Clone the Repository](#step-1-clone-the-repository)
    2. [Step 2: Build the Docker Image](#step-2-build-the-docker-image)
    3. [Step 3: Prepare the Configuration File](#step-3-prepare-the-configuration-file)
    4. [Step 4: Kafka ACLs Required](#step-4-kafka-acls-required)
    5. [Step 4: Set up Redis](#step-5-set-up-redis)
    6. [Step 5: Run the Docker Container](#step-5-run-the-docker-container)
7. [Configuration](#configuration)
8. [Running Unit and End-to-End (E2E) Tests](#running-unit-and-end-to-end-e2e-tests)
    1. [Unit Tests](#unit-tests)
    2. [End-to-End (E2E) Tests](#end-to-end-e2e-tests)
9. [Health Check Feature](#health-check-feature)
10. [Prometheus Metrics](#prometheus-metrics-overview)
11. [Next Steps](#next-steps)
12. [License](#license)

# Kafka Lag Go

Kafka Lag Go is a lightweight, stateless application designed to calculate Kafka consumer group lag in both offsets and seconds. It efficiently processes Kafka consumer group data using a pipeline pattern implemented with Go’s goroutines and channels, ensuring high performance and scalability. The application employs consistent hashing to distribute work evenly among multiple nodes, making it suitable for deployment in distributed environments.

The method for estimating lag in seconds used by Kafka Lag Go is inspired by the implementation found in the [Kafka Lag Exporter](https://github.com/seglo/kafka-lag-exporter) project, which is currently archived. Kafka Lag Go uses both interpolation and extrapolation techniques to approximate the lag in seconds for each partition within a Kafka consumer group.

This strategy provides a reasonable estimate of the time delay between message production and consumption, even in cases where the offset data is sparse or not evenly distributed. However, it is important to note that this is an approximation and not an exact measure, making it useful for gaining insights into lag trends rather than precise calculations.

## Features

- Offset Lag Calculation: Accurately computes the difference between the latest produced offset and the committed offset for each partition, topic, and consumer group.
- Time Lag Calculation: Calculates the lag in seconds, offering insight into the time delay between message production and consumption.
- Max Lag Calculation: Determines the maximum lag in offsets and seconds at both the consumer group and topic levels, providing a clear view of the most delayed parts of your system.
- Stateless Service: Designed to be stateless, allowing for easy scaling and distribution of work across multiple instances or nodes.
-	Multi-Cluster Ready: Supports simultaneous monitoring of multiple Kafka clusters, providing flexibility and scalability for complex environments.
- Consistent Hashing: Uses consistent hashing to split the workload among nodes, ensuring an even distribution of data processing tasks.
- Docker & Kubernetes Ready: Can be deployed as a Docker container, making it easy to run in Kubernetes or other container orchestration systems, as well as standalone.
- Pipeline Usage in Redis: The application utilizes Redis pipelines to reduce round trip times and improve overall performance when interacting with Redis. By batching multiple commands together and sending them in a single network request, the application minimizes the overhead of multiple round trips.


# Architecture Overview

This project is designed to efficiently manage data flow and processing tasks using Go's concurrency model. For a detailed explanation of the architecture, please refer to the [Architecture Documentation](docs/Architecture.md).

## Performance

In performance tests against other open-source solutions, Kafka Lag Calculator has demonstrated significant improvements, processing Kafka consumer group lag up to 18 times faster. These improvements are achieved through efficient use of Go’s concurrency model, consistent hashing for load distribution, and the lightweight, stateless architecture of the application.

### Prerequisites

- Docker (optional, it can run in standalone as well)
- A running Redis instance (for now the only supported storage)
- A running Kafka cluster

## Platform Compatibility

These images are available on Docker Hub and can be pulled and run on systems with the corresponding platforms:

- Linux/amd64
- Linux/arm64
- Linux/arm/v7

If you require Kafka Lag Calculator to run on other architectures or platforms, you can easily compile the application from the source code to suit your needs. The flexibility of the Go programming language ensures that Kafka Lag Calculator can be built and run virtually anywhere Go is supported.


## Installation

## Deployment Options

To start monitoring Kafka consumer group lag with Kafka Lag Monitor, you have the following deployment options:

1. Standalone Mode:
  - Run the service directly on your machine by compiling the source code and executing the binary.

2. Containerized Deployment:
  - Kafka Lag Monitor is fully containerized, making it easy to deploy in containerized environments like Kubernetes (K8s).
  - Build the Docker container yourself or pull the pre-built image from Docker Hub.


### Step 1: Clone the Repository

Begin by cloning the Kafka Lag Monitor repository:

```bash
git clone https://github.com/sciclon2/kafka-lag-go.git
cd kafka-lag-go
```

### Step 2: Build the Docker Image

Use the following command to build the Docker image for Kafka Lag Monitor:

```bash
docker build -t kafka-lag-go:latest .
```

### Step 3: Prepare the Configuration File

Kafka Lag Monitor requires a YAML configuration file. Create a file named `config.yaml` and customize it based on your environment. Below is an example configuration:

```yaml
prometheus_local:
  metrics_port: 9090
  labels:
    env: production
    service: kafka-lag-go

prometheus_remote_write:
  enabled: true
  url: "https://remote-prometheus-server.com/"
  headers:
    X-Custom-Header: "myCustomHeaderValue" 
  timeout: "30s"
  basic_auth:
    username: "myUsername"
    password: "myPassword"
  bearer_token: "myBearerToken" 
  tls_config:
    enabled: true
    cert_file: "/path/to/cert.pem"
    key_file: "/path/to/key.pem"
    ca_cert_file: "/path/to/ca_cert.pem"
    insecure_skip_verify: true

kafka_clusters:
  - name: "kafka-cluster-1"
    brokers:
      - "broker1:9092"
      - "broker2:9092"
    client_request_timeout: "30s"
    metadata_fetch_timeout: "5s"
    consumer_groups:
      whitelist: ".*"
      blacklist: "test.*"
    ssl:
      enabled: true
      client_certificate_file: "/path/to/cert.pem"
      client_key_file: "/path/to/key.pem"
      insecure_skip_verify: true
    sasl:
      enabled: true
      mechanism: "SCRAM-SHA-512"
      user: "kafkaUser1"
      password: "kafkaPassword1"

  - name: "kafka-cluster-2"
    brokers:
      - "broker3:9092"
      - "broker4:9092"
    client_request_timeout: "40s"
    metadata_fetch_timeout: "6s"
    consumer_groups:
      whitelist: "important.*"
      blacklist: "test2.*"
    ssl:
      enabled: false
    sasl:
      enabled: false

storage:
  type: "redis"
  redis:
    address: "redis-server"
    port: 6379
    client_request_timeout: "60s"
    client_idle_timeout: "5m"
    retention_ttl_seconds: 7200
    auth:
      enabled: true
      username: "redisUser"
      password: "redisPassword"
    ssl:
      enabled: true
      insecure_skip_verify: true

app:
  iteration_interval: "30s"
  num_workers: 10
  log_level: "info"
  health_check_port: 8080
  health_check_path: "/healthz"
```

### Step 4: Kafka ACLs Required

The `kafka-lag-go` application requires certain permissions to interact with Kafka and perform operations like fetching metadata, reading consumer group offsets, and describing topics. To ensure secure communication and limited access, appropriate **Access Control Lists (ACLs)** need to be applied. These permissions are necessary for the application to gather the metrics and information it needs from Kafka.

For detailed instructions on how to set up these ACLs, please refer to the [ACLs Documentation](docs/ACLs.md).

### Step 5: Set up Redis

To configure Redis for Kafka Lag Go, ensure your Redis server is properly set up. Kafka Lag Go supports authentication, authorization (ACL), and TLS for secure connections starting with Redis version 6.0.

- **Basic Setup**: If using Redis without authentication or encryption, you only need to provide the Redis address and port in the configuration file.
- **Advanced Setup**: For more secure environments, you can enable Redis authentication, ACLs, and TLS in the configuration.

For detailed steps on setting up Redis with these features, please refer to [the Redis setup documentation](docs/Redis.md).


### Step 5: Run the Docker Container

After building the image and preparing the configuration file, run the Kafka Lag Monitor Docker container:

```bash
docker run --rm -v /path/to/config.yaml:/app/config.yaml kafka-lag-monitor:latest --config-file /app/config.yaml
```

Replace `/path/to/config.yaml` with the actual path to your configuration file.


### (Or) Downlaod the image 
Kafka Lag Calculator is available as a Docker image, making it easy to deploy in containerized environments like Kubernetes.
You can download the Docker image from Docker Hub using the following command:
```
docker pull sciclon2/kafka-lag-go
```

## Configuration

The Kafka Lag Monitor requires a YAML configuration file to customize its behavior. Below is a description of the available configuration options:

- **Prometheus Metrics**:
  - `prometheus_local.metrics_port`: The port on which Prometheus metrics will be exposed locally.
  - `prometheus_local.labels`: Additional labels to be attached to the exported metrics.
  - `prometheus_remote_write`: Configuration for Prometheus Remote Write to export metrics remotely.
    - `url`: The URL of the Prometheus remote write endpoint.
    - `headers`: Optional HTTP headers (e.g., custom headers) for the remote write request.
    - `timeout`: Timeout duration for sending the metrics.
    - **Authentication**:
      - `basic_auth`: For basic authentication, specify:
        - `username`: The username for basic auth.
        - `password`: The password for basic auth.
      - `bearer_token`: If using bearer token authentication, specify the token here.
    - **TLS Settings**:
      - `tls_config.enabled`: Whether TLS encryption is enabled for Prometheus remote write.
      - `tls_config.cert_file`: Path to the client certificate file.
      - `tls_config.key_file`: Path to the client key file.
      - `tls_config.ca_cert_file`: Path to the CA certificate file for verifying the server's certificate.
      - `tls_config.insecure_skip_verify`: Whether to skip TLS certificate verification.

- **Kafka Clusters**:
  - `kafka_clusters`: An array of Kafka cluster configurations, each with the following options:
    - `name`: The name of the Kafka cluster.
    - `brokers`: The list of Kafka broker addresses.
    - `client_request_timeout`: The timeout for Kafka client requests.
    - `metadata_fetch_timeout`: The timeout for fetching Kafka metadata.
    - `consumer_groups.whitelist`: A regular expression to whitelist consumer groups.
    - `consumer_groups.blacklist`: A regular expression to blacklist consumer groups.
    - **SSL Settings**:
      - `ssl.enabled`: Whether SSL/TLS is enabled for Kafka connections.
      - `ssl.client_certificate_file`: The path to the client certificate file for SSL/TLS.
      - `ssl.client_key_file`: The path to the client key file for SSL/TLS.
      - `ssl.insecure_skip_verify`: Whether to skip SSL/TLS verification.
    - **SASL Settings**:
      - `sasl.enabled`: Whether SASL authentication is enabled for Kafka connections.
      - `sasl.mechanism`: The SASL mechanism to use (`SCRAM-SHA-256` or `SCRAM-SHA-512`).
      - `sasl.user`: The username for SASL authentication.
      - `sasl.password`: The password for SASL authentication.

- **Storage Configuration**:
  - `storage.type`: The type of storage backend to use (e.g., `redis`).
  - **Redis Settings**:
    - `storage.redis.address`: The address of the Redis server for Redis storage.
    - `storage.redis.port`: The port of the Redis server for Redis storage.
    - `storage.redis.client_request_timeout`: The timeout for Redis client requests.
    - `storage.redis.client_idle_timeout`: The idle timeout for Redis clients.
    - `storage.redis.retention_ttl_seconds`: The time-to-live (TTL) for Redis keys.
    - **Auth Settings (Redis 6.0+ support)**:
      - `storage.redis.auth.enabled`: Whether authentication is enabled for Redis.
      - `storage.redis.auth.username`: The username for Redis authentication.
      - `storage.redis.auth.password`: The password for Redis authentication.
    - **SSL Settings**:
      - `storage.redis.ssl.enabled`: Whether TLS encryption is enabled for Redis connections.
      - `storage.redis.ssl.insecure_skip_verify`: Whether to skip TLS verification.

- **Application Settings**:
  - `app.iteration_interval`: The interval at which the lag monitor iterates over consumer groups.
  - `app.num_workers`: The number of worker goroutines to use.
  - `app.log_level`: The log level for the lag monitor (`info`, `debug`, etc.).
  - `app.health_check_port`: The port on which the health check endpoint will be exposed.
  - `app.health_check_path`: The path of the health check endpoint.


Please refer to the `config.go` file for more details on each configuration option.

## Running Unit and End-to-End (E2E) Tests

### Unit Tests

We ensure code quality and functionality with a comprehensive suite of unit tests. These can be run locally on your laptop using the following command:

```bash
go test ./... -v -cover -count=1
```

- `-v`: Enables verbose output, providing detailed information about which tests are running.
- `-cover`: Generates a coverage report, showing how much of the code is covered by the tests.
- `-count=1`: Disables test result caching, ensuring that the tests run fresh every time.

### End-to-End (E2E) Tests

The E2E tests ensure that the full system operates as expected by validating the successful export of key metrics, confirming that these metrics reach their final destinations in either Prometheus Local or Prometheus Remote Write.

For more detailed information on the E2E test setup and execution, please see the [E2E Test Documentation](docs/e2e-tests.md).

## Health Check Feature

Kafka Lag Go includes a built-in health check system that continuously monitors the health of Kafka clusters and Redis. It ensures that the application can connect to these components and that they are functioning properly.

### What to Expect:

- The health check monitors both Kafka and Redis.
- If at least one Kafka cluster and Redis are running smoothly, the system is considered healthy.
- The health status is accessible through a dedicated HTTP endpoint, providing real-time information on system health.
- The endpoint will always return a `200 OK` status, with details on the system’s health in the response body:
  - `status`: "OK" if everything is functioning well, or "Unhealthy" if there’s an issue.
  - `unhealthy_clusters`: A list of Kafka clusters that are not performing correctly.
- The health check runs at regular intervals, ensuring up-to-date status information.

This feature allows you to easily monitor the health of your Kafka and Redis components and react promptly to any issues.


## Prometheus Metrics

This application exposes a comprehensive set of Prometheus metrics that provide insights into the lag experienced by Kafka consumer groups at both the group and topic levels. These metrics help monitor the health and performance of your Kafka consumers.

### Metrics Overview

- **`kafka_consumer_group_lag_in_offsets (group, topic, partition)`**: The lag in offsets for a specific partition within a Kafka topic for a consumer group. *(Type: Gauge)*
- **`kafka_consumer_group_lag_in_seconds (group, topic, partition)`**: The lag in seconds for a specific partition within a Kafka topic for a consumer group. *(Type: Gauge)*
- **`kafka_consumer_group_max_lag_in_offsets (group)`**: The maximum lag in offsets across all topics and partitions within a Kafka consumer group. *(Type: Gauge)*
- **`kafka_consumer_group_max_lag_in_seconds (group)`**: The maximum lag in seconds across all topics and partitions within a Kafka consumer group. *(Type: Gauge)*
- **`kafka_consumer_group_topic_max_lag_in_offsets (group, topic)`**: The maximum lag in offsets for a specific Kafka topic within a consumer group. *(Type: Gauge)*
- **`kafka_consumer_group_topic_max_lag_in_seconds (group, topic)`**: The maximum lag in seconds for a specific Kafka topic within a consumer group. *(Type: Gauge)*
- **`kafka_consumer_group_sum_lag_in_offsets (group)`**: The sum of lag in offsets across all topics and partitions within a Kafka consumer group. *(Type: Gauge)*
- **`kafka_consumer_group_sum_lag_in_seconds (group)`**: The sum of lag in seconds across all topics and partitions within a Kafka consumer group. *(Type: Gauge)*
- **`kafka_consumer_group_topic_sum_lag_in_offsets (group, topic)`**: The sum of lag in offsets for a specific Kafka topic within a consumer group. *(Type: Gauge)*
- **`kafka_consumer_group_topic_sum_lag_in_seconds (group, topic)`**: The sum of lag in seconds for a specific Kafka topic within a consumer group. *(Type: Gauge)*
- **`kafka_total_groups_checked`**: The total number of Kafka consumer groups checked in each iteration. *(Type: Gauge)*
- **`kafka_iteration_time_seconds`**: The time taken to complete an iteration of checking all Kafka consumer groups. *(Type: Gauge)*

Once the container is running, Kafka Lag Monitor will expose Prometheus metrics on the port specified in the configuration file (`metrics_port`). You can access the metrics at:

```
http://<docker-host-ip>:<metrics_port>/metrics
```

## Next Steps
Please check issues section.
For more details on usage and advanced configuration, refer to the full documentation (coming soon).

## License

This project is licensed under the Apache License 2.0. You may obtain a copy of the License at:

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.