package org.myflinkapp.operators;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.myflinkapp.models.TaxiStats;
import org.myflinkapp.service.metrics.MetricsLogger;

public class AverageSpeedCalculator extends RichFlatMapFunction<TaxiStats, TaxiStats> {
    private static final double MIN_TIME_SECONDS = 2.0;
    private static final double MIN_DISTANCE_KM = 0.01;
    private static final double MAX_REASONABLE_SPEED_KMPH = 300.0;

    private transient MetricsLogger metricsLogger;

    @Override
    public void open(Configuration parameters) {
        metricsLogger = new MetricsLogger("flink-consumer", "averageSpeedCalculator", 5);
    }

    @Override
    public void flatMap(TaxiStats stats, Collector<TaxiStats> out) {
        long start = System.nanoTime();

        try {
            double totalDistance = stats.getTotalDistanceKm(); // in km
            long totalTimeSeconds = stats.getTotalTimeSeconds(); // in seconds

            if (totalTimeSeconds < MIN_TIME_SECONDS || totalDistance < MIN_DISTANCE_KM) {
                System.out.println("Skipping average speed calc: time=" + totalTimeSeconds + "s, distance="
                        + totalDistance + "km for TaxiID=" + stats.getTaxiId());
                return;
            }

            double averageSpeed = totalDistance / (totalTimeSeconds / 3600.0); // km/h
            averageSpeed = Math.round(averageSpeed * 100.0) / 100.0;

            if (averageSpeed > MAX_REASONABLE_SPEED_KMPH) {
                System.out.println("Unrealistic speed: " + averageSpeed + " km/h for TaxiID=" + stats.getTaxiId());
                return;
            }

            stats.setAverageSpeedKmph(averageSpeed);
            out.collect(stats);


        } catch (Exception e) {
            metricsLogger.recordError("Exception for TaxiID=" + stats.getTaxiId() + ": " + e.getMessage());
        }
    }
}
