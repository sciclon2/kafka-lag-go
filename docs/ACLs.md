# Kafka Access Control Lists (ACLs) for `kafka-lag-go`

In order for `kafka-lag-go` to function properly, several permissions need to be granted at both the topic and group level within your Kafka cluster. These permissions are enforced via Access Control Lists (ACLs). Below, we outline the required ACLs, the operations they correspond to, and the commands to apply them using Kafka’s ACL management tools.

---

## Table of Contents

1. [Introduction](#introduction)
2. [Required ACLs](#required-acls)
    - [Kafka Client Permissions](#kafka-client-permissions)
    - [Kafka Admin Permissions](#kafka-admin-permissions)
3. [Applying ACLs](#applying-acls)
    - [Cluster Level ACLs](#cluster-level-acls)
    - [Topic Level ACLs](#topic-level-acls)
    - [Group Level ACLs](#group-level-acls)
4. [Example Commands](#example-commands)

---

## Introduction

The `kafka-lag-go` application requires specific Kafka ACLs to perform operations related to interacting with brokers, topics, partitions, and consumer groups. This document details the ACLs needed for both the Kafka client and admin interfaces used by the application.

---

## Required ACLs

### Kafka Client Permissions

The Kafka client interface requires the following permissions:

| Operation                  | Permission  | Resource Type | Description                                                        |
|----------------------------|-------------|---------------|--------------------------------------------------------------------|
| **Brokers()**               | DESCRIBE    | Cluster       | Allows fetching metadata of brokers.                               |
| **GetOffset()**             | DESCRIBE/READ | Topic       | Allows fetching offsets for specific topic partitions.             |
| **Leader()**                | DESCRIBE    | Topic         | Allows describing the leader broker for a partition in a topic.    |
| **RefreshMetadata()**       | DESCRIBE    | Topic         | Allows refreshing metadata for specific topics.                    |

### Kafka Admin Permissions

The Kafka admin interface requires the following permissions:

| Operation                               | Permission  | Resource Type | Description                                                        |
|-----------------------------------------|-------------|---------------|--------------------------------------------------------------------|
| **ListConsumerGroups()**                | DESCRIBE    | Group         | Allows listing of all consumer groups.                             |
| **DescribeTopics()**                    | DESCRIBE    | Topic         | Allows describing topics and their metadata.                       |
| **ListConsumerGroupOffsets()**          | DESCRIBE/READ | Group/Topic  | Allows listing offsets for a consumer group.                       |
| **ListTopics()**                        | DESCRIBE    | Topic         | Allows listing all topics and their details.                       |

---

## Applying ACLs

### Cluster Level ACLs
To enable the `kafka-lag-go` application to fetch metadata of brokers, apply the following ACL at the cluster level:

```bash
# Grant DESCRIBE permission for the entire Kafka cluster
kafka-acls --bootstrap-server kafka-broker:9092 \
  --add --allow-principal User:kafka-lag-go \
  --operation DESCRIBE --cluster
```

### Topic Level ACLs
To allow the application to describe topics, fetch offsets, and perform operations on partitions, apply the following ACLs for topics:

```bash
# Apply DESCRIBE and READ ACLs for all topics
kafka-acls --bootstrap-server kafka-broker:9092 \
  --add --allow-principal User:kafka-lag-go \
  --operation DESCRIBE --topic '*' \
  --operation READ --topic '*'
```

### Group Level ACLs    
The following ACLs allow kafka-lag-go to list and fetch offsets for consumer groups:

```bash
# Apply DESCRIBE and READ ACLs for all consumer groups
kafka-acls --bootstrap-server kafka-broker:9092 \
  --add --allow-principal User:kafka-lag-go \
  --operation DESCRIBE --group '*' \
  --operation READ --group '*'
```


### Listing Existing ACLs
```bash
kafka-acls --bootstrap-server kafka-broker:9092 --list
```