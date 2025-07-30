#!/bin/bash

KAFKA_HOST="kafka"
KAFKA_PORT="9092"

echo "Waiting for Kafka to be ready..."
while ! python -c "import socket; s = socket.socket(); s.connect((\"$KAFKA_HOST\", int($KAFKA_PORT)))" >/dev/null 2>&1; do
  echo "Kafka is not ready yet..."
  sleep 2
done

echo "Kafka is ready! Starting producer..."
python producer.py