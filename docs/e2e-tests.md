## End-to-End (E2E) Tests

### Overview

End-to-End (E2E) tests are essential to validate the overall system behavior by simulating real-world scenarios. In Kafka-Lag-Go, these tests cover the entire pipeline, ensuring that data flows correctly from Kafka to Redis and, finally, to Prometheus for metric collection and visualization.

### What Does Kafka-Lag-Go Do?

Kafka-Lag-Go collects data from Kafka, stores relevant information in Redis, and generates metrics that can be either scraped locally or pushed to a central Prometheus instance using Prometheus Remote Write.

#### Differences Between Local Prometheus Scrape and Prometheus Remote Write

- **Local Prometheus Scrape (`/metrics` endpoint)**: This method captures a snapshot of the current state of the system's metrics at any given time when Prometheus performs its scheduled scrapes. Metrics are queried directly from the local endpoint, which gives the latest values for the tracked metrics.
  
- **Prometheus Remote Write**: In contrast, this approach pushes metrics to a central Prometheus instance at regular intervals. Remote Write allows for scalable and centralized monitoring, where multiple services or distributed instances can send their metrics to a single, central Prometheus server for aggregation.

### E2E Test Structure

The E2E tests for Kafka-Lag-Go are housed in the `test/e2e/` directory. This folder contains all necessary components for each E2E test, including configuration files for Docker Compose and Go-based test files. The main script `run.sh` is used to automate the execution of each test, taking the directory of the specific E2E test as an argument.

Example structure:

- `test/run.sh`: Main script to run tests.
- `test/e2e/tests/`: Directory holding individual E2E tests.
- `test/e2e/tests/[test_name]/`: Each test has its own folder with Docker Compose setup and test configurations.

The tests are run using the `run.sh` script, which ensures that the infrastructure (Kafka, Redis, etc.) is correctly set up before executing the tests. For example:
```bash
$ test/run.sh test/e2e/tests/prometheus_local -d 
```

### Automating E2E Tests in GitHub Actions

In the GitHub Actions workflow, E2E tests are automatically detected and executed in isolated jobs. Each test folder is run in a separate job to ensure proper isolation since different tests may start their own infrastructure.

When a new folder is added under `test/e2e/tests/`, it will be automatically picked up by the GitHub workflow and executed as part of the CI pipeline. Each test's infrastructure is provisioned via Docker Compose, and the tests are executed using the Go test files located within each folder.

### Adding a New E2E Test

To add a new E2E test:

1. Copy an existing folder inside `test/e2e/tests/` and modify it according to your test's needs.
2. Update the Docker Compose configuration with any necessary components (e.g., Kafka, Redis, Nginx).
3. Write a new E2E test in Go and place it inside the `go_test/` folder.

You can test the new E2E test locally using the `run.sh` script by pointing it to the test folder:
```bash
$ cp -a test/e2e/tests/prometheus_remote_write_basic_auth test/e2e/tests/my-new-cool-test
$ # make sure the docker compose file has all the needed services
$ # edit your go_test/e2e_test.go
$ # finally run the test locally
$ test/e2e/run.sh test/e2e/tests/my-new-cool-test -d # -d is for debuging
```

E2E tests are automatically triggered when you merge to the main branch or create a new release.