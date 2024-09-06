# Changelog

## [v2.0.1] - Minor improvements (Latest)

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