# Changelog

## [v3.0.0] - Prometheus Remote Write Support and E2E Test Enhancements (12th Sept 2024)

### Breaking Changes
- **Configuration File Format**: 
  - Introduced a new structure in the configuration file to separate Prometheus local scraping and Prometheus Remote Write.
  - Users now need to update their `config.yaml` file to accommodate the new structure, including new sections for `prometheus_local` and `prometheus_remote_write`.

### Feat
- **Prometheus Remote Write**: 
  - Added support for Prometheus Remote Write, allowing metrics to be sent directly to a remote Prometheus instance.
  - The remote write feature supports both **Basic Auth** and **Bearer Token** authentication, as well as optional **TLS configuration** for secure communication.
- **Prometheus Local and Remote Split**: 
  - Split the original `prometheus.go` file into two separate files: one for **Prometheus Local** metrics and one for **Prometheus Remote Write**, improving clarity and maintainability.
- **E2E Test Enhancements**: 
  - Created a new structure for **self-discovering end-to-end tests** that run in isolated environments. Each E2E test is automatically detected and executed in a separate job in GitHub Actions for better isolation and parallelism.
  - Added a specific **E2E test for Prometheus Remote Write** to ensure the metrics are correctly sent and received by the remote Prometheus server.

### Documentation
- **Prometheus Documentation**: 
  - Updated the documentation with details on how to configure **Prometheus Remote Write**, including **Basic Auth**, **Bearer Token**, and **TLS** settings.
  - Provided examples for both local and remote Prometheus configurations, helping users understand how to transition to the new structure.
  
---

## [v2.0.2] - Redis Authentication, Authorization, and TLS Support (10th Sept 2024)

### Feat
- **Redis AuthN/AuthZ and TLS Support**: Added support for Redis authentication (AuthN) and authorization (AuthZ) using ACLs (available from Redis 6.0+). This enables users to provide usernames and passwords for secure access to Redis instances.
- **Redis TLS Support**: Introduced TLS encryption for Redis connections, allowing secure communication between Kafka Lag Go and Redis clusters.
- **Backward Compatibility**: These features maintain full backward compatibility, supporting unsecured Redis instances for users who prefer non-encrypted connections.

### Documentation
- **Redis Setup Guide**: Updated the Redis documentation to include configuration examples for setting up authentication, authorization, and TLS.

---

# [v2.0.1] - Minor improvements (Latest)

### Feat
- **Logging Enhancements**: Improved both debug and info log levels for better visibility, including additional context such as `clusterName` in the logs.
- **Configuration Example**: Added a `config.yaml` example file to guide configuration setup.

### Fix
- **Race Condition**: Fixed a race condition in `TestPersistLatestProducedOffsetsMultipleClusters`, ensuring predictable order in results.
- **Mermaid Diagram**: Corrected the broken Mermaid diagram in the architecture documentation to accurately reflect the application’s flow.
- **E2E Test**: Updated the end-to-end tests to properly use their own configuration file (`config.yaml`), ensuring isolated and correct test runs.

---

## [v2.0.0] - Breaking Change: Multi-Cluster Kafka Support

### Feat
- **Multi-Cluster Support**: Introduced support for multiple Kafka clusters, changing the configuration file format to allow defining multiple Kafka clusters in an array under the `kafka_clusters` key.
  
### Breaking Changes
- Removed support for the previous configuration format that assumed a single Kafka cluster.

---

## [v1.x.x] - Initial Release

### Breaking Changes
- **Single Kafka Cluster Support**: Initial release supported only a single Kafka cluster.