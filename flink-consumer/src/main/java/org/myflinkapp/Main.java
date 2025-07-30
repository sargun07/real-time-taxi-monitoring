package org.myflinkapp;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import org.myflinkapp.models.*;
import org.myflinkapp.operators.*;
import org.myflinkapp.utils.*;
import org.myflinkapp.service.redis.RedisTaxiStatsWriter;
import org.myflinkapp.service.redis.RedisCacheManager;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.time.Time;

import static org.myflinkapp.operators.CalculateSpeedFunction.enrichedOutputTag;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import org.myflinkapp.service.metrics.MetricsLogger;


import java.time.Duration;
import java.util.Properties;

public class Main {
    private static transient MetricsLogger metricsLogger;

    public static void main(String[] args) throws Exception {

        // // Set up Flink environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //private transient MetricsLogger metricsLogger;

        // Get Kafka consumer from config class
        FlinkKafkaConsumer<TaxiLocation> consumer = KafkaConfig.createTaxiLocationConsumer();

        // Add watermark to prevent out of order taxi ids.
        DataStream<TaxiLocation> stream = env.addSource(consumer).setParallelism(1)  
        .assignTimestampsAndWatermarks(WatermarkStrategy
        .<TaxiLocation>forBoundedOutOfOrderness(Duration.ofMinutes(2))  
        .withTimestampAssigner((event, timestamp) -> event.getTimestamp()));

        //stream.print("Raw Kafka data");

        // Clear Redis before starting Flink job
        RedisCacheManager.getInstance().clearTaxiData();

        metricsLogger = new MetricsLogger("flink-consumer", "endToEndLatency", 5);

        // Add shutdown hook to close Redis connection on job termination
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown hook triggered: closing Redis connection.");
            RedisCacheManager.shutdown();
        }));


        // // To check number of taxi id's are out of order.
        // DataStream<TaxiLocation> checkedStream = stream.keyBy(TaxiLocation::getTaxiId).flatMap(new TimestampOrderChecker());

        // Calculate distance from the center of the forbidden city and filter the stream where taxi id's are not in order
        //DataStream<GeoTaggedTaxiLocation> geoTaggedStream = stream.map(new AddDistanceFromCenterFunction());
        DataStream<GeoTaggedTaxiLocation> geoTaggedStream = stream.keyBy(TaxiLocation::getTaxiId).flatMap(new AddDistanceFromCenterFunction());

        // Filter the stream where taxi goes beyond the radius > 15 and send warning and red alerts for taxi's distance greater than 10kms and 15kms respectively.
        DataStream<TaxiLocation> filteredStream = geoTaggedStream.keyBy(e -> e.getLocation().getTaxiId()).flatMap(new ViolationFilterAndAlertFunction());
        
        // Calculate total distance travelled by the taxi; if the status of the taxi is removed do not save the state of the variables; update total distance of the taxi and total distance travelled by all the taxi in redis
        DataStream<TaxiStats> distanceStream = filteredStream.keyBy(TaxiLocation::getTaxiId).flatMap(new DistanceCalculator());

        // Calculate average speed of the taxi.
        DataStream<TaxiStats> statsStream = distanceStream.keyBy(TaxiStats::getTaxiId).flatMap(new AverageSpeedCalculator());

        // updating taxi location - latitude, longitude, timestamp, average speed and status - active/ inactive in redis; it also logs when the taxi is active, inactive or removed
        statsStream.map(stat -> {
            RedisTaxiStatsWriter.writeStats(stat);
            long eventTimeMillis = (long)(stat.getProducedTimestamp() * 1000); // assuming seconds
            long processingTimeMillis = System.currentTimeMillis();
            long latencyMillis = processingTimeMillis - eventTimeMillis;

            System.out.println("End-to-end Latency for TaxiID " + stat.getTaxiId() + ": " + latencyMillis + " ms");
            metricsLogger.recordSuccess(latencyMillis);
            return stat;
        });

        // Calculate the speed of the taxi.
        SingleOutputStreamOperator<TaxiSpeed> speedStream = filteredStream.keyBy(TaxiLocation::getTaxiId).process(new CalculateSpeedFunction());

        // Extract EnrichedTaxiSpeed from side output
        DataStream<EnrichedTaxiSpeed> enrichedStream = speedStream.getSideOutput(CalculateSpeedFunction.enrichedOutputTag);

        // updating speeding information in the redis
        speedStream.map(speed -> {
            RedisTaxiStatsWriter.writeSpeedingStatus(speed);
            return speed;
        });

        // Send alert if the taxi is speeding.
        DataStream<String> speedAlerts = speedStream.flatMap(new SpeedViolationAlertFunction());

        // joining filteredStream and speedStream to get a new stream EnrichedTaxiData stream
        // DataStream<EnrichedTaxiData> enrichedStream = filteredStream
        // .assignTimestampsAndWatermarks(
        // WatermarkStrategy.<TaxiLocation>forBoundedOutOfOrderness(Duration.ofSeconds(10))
        //     .withTimestampAssigner((loc, ts) -> loc.getTimestamp())
        // )
        // .keyBy(TaxiLocation::getTaxiId)
        // .intervalJoin(
        //     speedStream
        //     .assignTimestampsAndWatermarks(
        //         WatermarkStrategy.<TaxiSpeed>forBoundedOutOfOrderness(Duration.ofSeconds(10))
        //             .withTimestampAssigner((speed, ts) -> speed.getTimestamp())
        //     )
        //     .keyBy(TaxiSpeed::getTaxiId)
        // )
        // .between(Time.seconds(-5), Time.seconds(5))
        // .process(new ProcessJoinFunction<TaxiLocation, TaxiSpeed, EnrichedTaxiData>() {
        // @Override
        // public void processElement(TaxiLocation loc, TaxiSpeed speed, Context ctx, Collector<EnrichedTaxiData> out) {
        //     String gridId = GridUtils.computeGridId(loc.getLatitude(), loc.getLongitude());

        //     EnrichedTaxiData enriched = new EnrichedTaxiData(
        //         loc.getTaxiId(),
        //         loc.getTimestamp(),
        //         loc.getLatitude(),
        //         loc.getLongitude(),
        //         speed.getSpeedKmph(),
        //         speed.isSpeeding(),
        //         loc.isEndFlag()
        //     );

        //     enriched.setGridId(gridId); 

        //     out.collect(enriched);
        // }
        // });

        enrichedStream.keyBy(EnrichedTaxiSpeed::getGridId).process(new TrafficGridUpdater()); 



        // Print total distance and average speed in the logs.
        statsStream.map(stat -> "Taxi ID: " + stat.getTaxiId() +
                        " | Total Distance: " + stat.getTotalDistanceKm() + " km" + 
                        "| Average Speed: " + stat.getAverageSpeedKmph() + " km/h").print().setParallelism(1);

        // Execute the pipeline
        env.execute("Flink Kafka Reader");
    }
}
