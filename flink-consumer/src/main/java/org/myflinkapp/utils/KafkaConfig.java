package org.myflinkapp.utils;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.myflinkapp.models.TaxiLocation;

import java.util.Properties;
import java.util.UUID;

public class KafkaConfig {

    public static Properties getProperties() {
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "kafka:9092");
    properties.setProperty("group.id", "flink-taxi-consumer-" + UUID.randomUUID());
    properties.setProperty("auto.offset.reset", "earliest");
    return properties;
}

    public static FlinkKafkaConsumer<TaxiLocation> createTaxiLocationConsumer() {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:9092");
        properties.setProperty("group.id", "flink-taxi-consumer-" + UUID.randomUUID());
        properties.setProperty("auto.offset.reset", "earliest");

        FlinkKafkaConsumer<TaxiLocation> consumer = new FlinkKafkaConsumer<>(
            "taxi-location",
            new TaxiLocationDeserialization(),
            properties
        );

        return consumer;
    }

}
