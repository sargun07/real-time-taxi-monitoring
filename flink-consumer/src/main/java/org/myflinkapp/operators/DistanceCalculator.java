package org.myflinkapp.operators;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.myflinkapp.models.TaxiLocation;
import org.myflinkapp.models.TaxiStats;
import org.myflinkapp.service.redis.RedisTaxiStatsWriter;
import org.myflinkapp.service.metrics.MetricsLogger;

public class DistanceCalculator extends RichFlatMapFunction<TaxiLocation, TaxiStats> {

    private transient ValueState<TaxiLocation> lastLocationState;
    private transient ValueState<Double> totalDistanceState;
    private transient ValueState<Long> totalTimeState;
    private transient ValueState<Long> totalHaltedTimeState;
    private transient ValueState<Boolean> isHaltedState;

    private transient MetricsLogger metricsLogger;

    @Override
    public void open(Configuration parameters) {
        lastLocationState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastLocation", TaxiLocation.class));

        totalDistanceState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("totalDistance", Double.class));
        totalTimeState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("totalTime", Long.class));
        totalHaltedTimeState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("totalHaltedTime", Long.class));
        isHaltedState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("isHalted", Boolean.class));

        metricsLogger = new MetricsLogger("flink-consumer", "distanceCalculator", 5);
    }

    @Override
    public void flatMap(TaxiLocation current, Collector<TaxiStats> out) throws Exception {
        long start = System.nanoTime();

        try {
            TaxiLocation last = lastLocationState.value();
            double totalDistance = totalDistanceState.value() != null ? totalDistanceState.value() : 0.0;
            long totalTimeSeconds = totalTimeState.value() != null ? totalTimeState.value() : 0L;
            String taxiId = current.getTaxiId();
            long totalHaltedTime = totalHaltedTimeState.value() != null ? totalHaltedTimeState.value() : 0L;
            boolean isHalted = isHaltedState.value() != null && isHaltedState.value();

            if (last != null) {
                double distance = haversine(
                        last.getLatitude(), last.getLongitude(),
                        current.getLatitude(), current.getLongitude());

                long timeDiffMillis = current.getTimestamp() - last.getTimestamp();
                long timeDiffSeconds = timeDiffMillis > 0 ? timeDiffMillis / 1000 : 0;
                totalTimeSeconds += timeDiffSeconds;

                if (distance > 0) {
                    totalDistance += distance;
                    totalDistance = Math.round(totalDistance * 100.0) / 100.0;
                    RedisTaxiStatsWriter.writeTotalDistance(taxiId, totalDistance);
                }

                if (distance < 0.01) {
                    totalHaltedTime += timeDiffSeconds;
                    totalHaltedTimeState.update(totalHaltedTime);

                    if (!isHalted && totalHaltedTime >= 180) {
                        RedisTaxiStatsWriter.markHalted(taxiId, true);
                        isHaltedState.update(true);
                    }
                } else {
                    if (isHalted) {
                        RedisTaxiStatsWriter.markHalted(taxiId, false);
                        isHaltedState.update(false);
                    }
                }
            }

            String status = RedisTaxiStatsWriter.getStatus(taxiId);

            if (current.isEndFlag() || "removed".equals(status)) {
                lastLocationState.clear();
                totalDistanceState.clear();
                totalTimeState.clear();
            } else {
                lastLocationState.update(current);
                totalDistanceState.update(totalDistance);
                totalTimeState.update(totalTimeSeconds);
            }

            TaxiStats stats = new TaxiStats(
                    current.getTaxiId(),
                    totalDistance,
                    totalTimeSeconds,
                    0.0 // averageSpeed placeholder
            );

            stats.setProducedTimestamp(current.getProducedTimestamp());

            stats.setLatestLocation(current);
            out.collect(stats);

        } catch (Exception e) {
            metricsLogger.recordError("Exception for TaxiID=" + current.getTaxiId() + ": " + e.getMessage());
            throw e;
        }
    }

    private double haversine(double lat1, double lon1, double lat2, double lon2) {
        final int R = 6371; // Earth radius in km
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                        * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c;
    }
}
