## Integration Tests

### Overview

Integration tests are crucial for validating specific components of the system and ensuring they work together correctly. In Kafka-Lag-Go, these tests focus on smaller parts of the system, such as Redis, Kafka, and other individual services, without involving the full pipeline that is tested in End-to-End (E2E) tests. Integration tests are typically faster and more targeted.

### What Do Integration Tests Cover?

In Kafka-Lag-Go, integration tests typically verify the functionality of individual components like Kafka and Redis, particularly focusing on how they interact with other parts of the system (e.g., Kafka Lag tracking, Redis Lua scripts for node management, etc.). These tests ensure that key components like Kafka and Redis perform their expected functions in isolation and as part of a broader workflow.

### Integration Test Structure

The integration tests for Kafka-Lag-Go are housed in the `test/integration/` directory. This folder contains all necessary components for each integration test, including configuration files for Docker Compose and Go-based test files. The main script `run.sh` is used to automate the execution of each test, taking the directory of the specific integration test as an argument.

#### Example structure:
- `test/run.sh`: Main script to run tests.
- `test/integration/tests/`: Directory holding individual integration tests.
- `test/integration/tests/[test_name]/`: Each test has its own folder with Docker Compose setup and test configurations.

The tests are run using the `run.sh` script, which ensures that the infrastructure (Kafka, Redis, etc.) is correctly set up before executing the tests. For example:

<!-- add code here -->

The `--only-start-infrastructure` flag can be used to start the infrastructure without running the Kafka-Lag-Go application, which is useful for integration tests where only the infrastructure (e.g., Redis, Kafka) is required. This flag allows you to avoid starting unnecessary components like the Kafka-Lag-Go app.

### Infrastructure for Each Test

The infrastructure for each integration test can be customized by editing the **Docker Compose** configuration files located in each test’s folder. This allows you to launch only the necessary services (e.g., Redis, Kafka) for each test. For example, if you are testing Redis Lua scripts, you may only need Redis, so you can customize the Docker Compose file to launch Redis only.
You can control the infrastructure and services launched per test based on the components that are relevant for the integration scenario.


The tests are run using the run.sh script, which ensures that the infrastructure (Kafka, Redis, etc.) is correctly set up before executing the tests. For example:

```bash
$ test/run.sh test/integration/tests/redis_lua_scripts --only-start-infrastructure
```

You can control the infrastructure and services launched per test based on the components that are relevant for the integration scenario.

### Automating Integration Tests in GitHub Actions

In the GitHub Actions workflow, integration tests are automatically detected and executed in isolated jobs. Each test folder is run in a separate job to ensure proper isolation, as different tests may start their own infrastructure.

When a new folder is added under `test/integration/tests/`, it will be automatically picked up by the GitHub workflow and executed as part of the CI pipeline. Each test's infrastructure is provisioned via Docker Compose, and the tests are executed using the Go test files located within each folder.

### Adding a New Integration Test

To add a new integration test:

1. Copy an existing folder inside `test/integration/tests/` and modify it according to your test’s needs.
2. Update the Docker Compose configuration with any necessary components (e.g., Kafka, Redis).
3. Write a new integration test in Go and place it inside the `go_test/` folder.

You can test the new integration test locally using the `run.sh` script by pointing it to the test folder:
```bash
$ cp -a test/integration/tests/redis_lua_scripts test/e2e/tests/my-new-cool-test
$ # make sure the docker compose file has all the needed services
$ # edit your go_test/integration_test.go
$ # finally run the test locally
$ test/run.sh test/integration/tests/my-cool-new-test --only-start-infrastructure
```

Integration tests are automatically triggered when you merge to the main branch or create a new release.