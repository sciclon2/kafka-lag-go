# Changelog


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