# Redis Configuration and Compatibility

## Overview

The `kafka-lag-go` application has introduced support for connecting to Redis using both insecure (unencrypted) and secure (TLS-encrypted) methods. It also supports authentication and authorization using Redis ACL, a feature available starting with Redis 6.0. 

This new feature provides flexibility, allowing you to secure your Redis connections in production while still supporting insecure configurations for development and legacy systems.

### Backward Compatibility

For environments where security is not the main concern or for compatibility with legacy Redis versions (before Redis 6.0), `kafka-lag-go` still supports:
- **Insecure Connections:** Plaintext communication between the application and Redis.
- **No Authentication or Authorization:** Unrestricted access to Redis without requiring a username and password.

```yaml
storage:
  type: "redis"
  redis:
    address: "redis-server"  # Redis server address
    port: 6379
    client_request_timeout: "60s"
    client_idle_timeout: "5m"
    retention_ttl_seconds: 7200
    auth:
      enabled: false  # No authentication
    ssl:
      enabled: false  # No TLS encryption
```

---

### New Features: Redis ACL and TLS (6.0+)

With Redis 6.0 and above, you can now enable:
- **Authentication (AuthN):** Redis ACLs allow you to create users with passwords, ensuring that only authorized clients can access the Redis server.
- **Authorization (AuthZ):** Control the commands and operations that specific users are allowed to perform on Redis, providing fine-grained access control.
- **TLS Encryption:** Secure communication between your application and Redis using TLS, ensuring that data is encrypted in transit.

#### How `kafka-lag-go` Handles Authentication and Authorization

`kafka-lag-go` handles Redis ACLs internally. As a user, you only need to configure your Redis instance by creating users with the appropriate permissions on your side. Then, provide the username and password in the `kafka-lag-go` Redis configuration.

Here’s how it works:
1. **Create a Redis User:** On your Redis server, create a user with full permissions (`ALL` commands or the specific commands needed for your use case).
2. **Configure `kafka-lag-go`:** In the `kafka-lag-go` Redis configuration, provide the Redis username and password.

```yaml
storage:
  type: "redis"
  redis:
    address: "redis-server"
    port: 6379
    client_request_timeout: "60s"
    client_idle_timeout: "5m"
    retention_ttl_seconds: 7200
    auth:
      enabled: true              # Enable authentication
      username: "redisUser"       # Redis ACL username
      password: "redisPassword"   # Redis ACL password
    ssl:
      enabled: true
      insecure_skip_verify: false  # Ensure server certificate verification
      ca_cert_file: "/path/to/ca-cert.pem"  # Optional: CA certificate for Redis TLS
```
---

### Example Use Cases

#### 1. **Insecure Redis for Development:**
   In development environments where security is not critical, you can use an insecure Redis connection without any authentication or encryption.

#### 2. **Secure Redis for Production:**
   In production environments, it is recommended to enable both Redis ACLs (for authentication and authorization) and TLS encryption to ensure secure communication and access control.

---

### Redis ACL and TLS Configuration

#### 1. **Creating Redis ACL Users:**
   In Redis 6.0+, use the `ACL SETUSER` command to create users and assign permissions. This ensures that your application can connect securely while only performing authorized operations.

#### 2. **Enabling TLS Encryption:**
   Configure your Redis server to support TLS by setting up certificates and keys. The corresponding TLS configurations can be added to the `kafka-lag-go` configuration.

---

### Summary

The introduction of Redis ACL and TLS support in `kafka-lag-go` enhances the security of your Redis connections in production environments. While backward compatibility with insecure Redis connections remains, this new feature allows you to enforce authentication, authorization, and encryption where needed, ensuring compliance with security best practices.