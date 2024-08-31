#!/bin/bash

# Variables
TOPIC_NAME="test-topic"
PARTITIONS=3
BROKER="localhost:9092"
GROUP_ID="test-consumer-group"
NUM_RECORDS=75
SLEEP_INTERVAL=25
SLEEP_DURATION=3

# Create topic
docker exec -it kafka-broker kafka-topics --create --topic $TOPIC_NAME --bootstrap-server $BROKER --partitions $PARTITIONS --replication-factor 1

# Produce records
for ((i=1; i<=NUM_RECORDS; i++))
do
  echo "Producing record $i"
  echo "message-$i" | docker exec -i kafka-broker kafka-console-producer --topic $TOPIC_NAME --bootstrap-server $BROKER

  # Sleep every $SLEEP_INTERVAL records
  if (( $i % $SLEEP_INTERVAL == 0 )); then
    echo "Sleeping for $SLEEP_DURATION seconds..."
    sleep $SLEEP_DURATION
  fi
done

# Consume records with a group ID
echo "Starting consumer with group ID: $GROUP_ID"
docker exec -it kafka-broker kafka-console-consumer --topic $TOPIC_NAME --bootstrap-server $BROKER --group $GROUP_ID --from-beginning --timeout-ms 10000

echo "Finished setting up Kafka with $NUM_RECORDS records."