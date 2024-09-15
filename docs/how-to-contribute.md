# How to Contribute

We welcome contributions to the Kafka Lag Go project, whether it’s reporting an issue, requesting a feature, or contributing code. Here are some guidelines to get you started:

## 1. Check the Documentation

Before contributing, take a look at the documentation in the `docs` folder. It includes valuable information such as:

- [Architecture Design](docs/Architecture.md): An overview of the system's design, which is helpful for understanding the codebase.
- [E2E Tests](docs/e2e-tests.md): Details on how our end-to-end testing framework works.

Understanding these documents will help you navigate the project and align your contribution with its goals.

## 2. Reporting an Issue

If you encounter a bug or have an idea for a new feature, please create an issue in the GitHub repository. When submitting an issue:

- **For Bugs**: Include as much information as possible, such as:
  - Steps to reproduce the issue
  - Expected behavior vs. actual behavior
  - Relevant logs or screenshots
  - Version of the application and environment details
  - **Use the Debug Flag**: Run the application with the `--debug` flag to capture additional diagnostic information. This will help us better understand the issue and speed up the resolution process.
  
- **For Feature Requests**: Provide a detailed description of the feature you would like to see added. Include any specific use cases or examples to clarify the request.

## 3. Contributing Code

If you'd like to contribute directly by fixing a bug or adding a feature, we encourage you to fork the repository and submit a pull request (PR).

### Steps for Contributing Code:

1. **Fork the Repository**: 
   - Fork the repo on GitHub and clone your fork to your local machine.

2. **Create a Branch**: 
   - Create a descriptive branch for your changes, for example:
     ```
     git checkout -b feature/new-awesome-feature
     ```

3. **Make Your Changes**: 
   - Add the necessary code changes to implement the feature or fix the bug.
   - Ensure you write the corresponding unit tests for your changes.
   - For larger features, add end-to-end (E2E) tests if applicable.

4. **Run Tests**: 
   - Before submitting your PR, make sure all unit tests and E2E tests pass by running them locally.
   - For unit tests:
     ```bash
     go test ./... -v -cover -count=1
     ```
   - For E2E tests, follow the instructions in the [E2E Test Documentation](docs/e2e-tests.md).

5. **Submit a Pull Request**: 
   - Push your changes to your forked repository and submit a pull request to the main repo.
   - Provide a clear description of your changes, linking to the relevant issue if it exists.

## 4. Writing Unit and E2E Tests

Every contribution that includes new functionality or changes should also include corresponding tests to ensure the stability of the project.

### Unit Tests
- **Unit tests** should cover the core functionality of the new feature or fix. They should be located in the corresponding package where the change is made.

### End-to-End (E2E) Tests
- **E2E tests** may be necessary if your contribution affects how the system interacts with external services such as Kafka, Redis, or Prometheus. These tests ensure the full integration works as expected.
- Refer to the `test/e2e/` directory for examples of existing E2E tests.

---

Thank you for your contributions! We look forward to your participation in the Kafka Lag Go project.