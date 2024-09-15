### Prometheus Remote Write Configuration

Prometheus Remote Write allows you to send metrics from one Prometheus instance to another for centralized storage or long-term retention. This can be especially useful in distributed systems like kafka-lag-go where metrics from various nodes need to be consolidated for analysis.

#### Benefits of Using Prometheus Remote Write
- **Centralized Monitoring**: With Remote Write, you can aggregate metrics from multiple Prometheus instances across different environments (e.g., development, production) into one central instance for unified monitoring and alerting.
- **Long-Term Retention**: Prometheus itself is designed for short-term storage. Remote Write allows metrics to be forwarded to a long-term storage system, ensuring that historical data is kept for analysis and audit purposes.
- **Scalability**: Remote Write enables organizations to scale their monitoring setups by offloading data from local Prometheus instances to a remote, centralized Prometheus or another storage backend.
- **Disaster Recovery**: By forwarding metrics to a remote system, you reduce the risk of data loss in the event that local Prometheus instances go down.

#### Example 1: Using Bearer Token Authentication

```yaml
prometheus_remote_write:
  enabled: true                    # Enable remote write
  url: "https://localhost:8443"     # Prometheus remote write URL
  timeout: "30s"                    # Remote write timeout
  bearer_token: "dummy_token"       # Bearer token for authentication
  tls_config:                       # Optional TLS configuration
    enabled: true
    insecure_skip_verify: true      # Skip server certificate verification
```

**Explanation**:
- `enabled`: Set to `true` to enable remote write.
- `url`: The URL of the Prometheus remote write endpoint.
- `timeout`: Time to wait for a remote write request to complete.
- `bearer_token`: The token used for bearer token authentication.
- `tls_config`: Optional settings for securing the connection with TLS.

#### Example 2: Using Basic Authentication

```yaml
prometheus_remote_write:
  enabled: true                    # Enable remote write
  url: "https://localhost:8443"     # Prometheus remote write URL
  timeout: "30s"                    # Remote write timeout
  basic_auth:                       # Basic Authentication
    username: "yourusername"
    password: "123456"
  tls_config:                       # Optional TLS configuration
    enabled: true
    insecure_skip_verify: true      # Skip server certificate verification
```

**Explanation**:
- `basic_auth`: This section configures basic authentication with `username` and `password`.
- TLS options are similar to the previous example, used to secure the connection.

#### Example 3: nor Authentication neither TLS

```yaml
prometheus_remote_write:
  enabled: true                    # Enable remote write
  url: "http://localhost:8081"      # Prometheus remote write URL
  timeout: "30s"                    # Remote write timeout
  tls_config:                       # TLS not enabled
    enabled: false                  # No TLS, plaintext communication
```
**Explanation**:
- This setup does not include any authentication or TLS. It is useful for internal, secure environments where additional security measures like authentication and encryption are not necessary.

### Further Details

For more information on how to configure Prometheus Remote Write, including advanced settings, please refer to the [official Prometheus documentation](https://prometheus.io/docs/specs/remote_write_spec/).
