FROM openjdk:8u151-jdk-alpine3.7

# Install Bash
RUN apk add --no-cache bash libc6-compat

# Copy resources
WORKDIR /
COPY wait-for-it.sh wait-for-it.sh
COPY target/flink-kafka2redis-1.0-SNAPSHOT-jar-with-dependencies.jar flink-consumer.jar

# Wait for Zookeeper and Kafka to be available and run application
CMD sh -c './wait-for-it.sh -s -t 120 "$KAFKA_SERVER" -- java -Xmx512m -jar flink-consumer.jar'
RUN chmod +x wait-for-it.sh