package org.myflinkapp.operators;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.myflinkapp.models.TaxiLocation;
import org.myflinkapp.models.TaxiSpeed;
import org.myflinkapp.service.redis.RedisTaxiStatsWriter;
import org.myflinkapp.utils.GridUtils;
import org.apache.flink.util.OutputTag;
import org.myflinkapp.models.EnrichedTaxiSpeed;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.myflinkapp.service.metrics.MetricsLogger;

public class CalculateSpeedFunction extends ProcessFunction<TaxiLocation, TaxiSpeed> {

    private transient ValueState<TaxiLocation> lastLocationState;
    private transient ValueState<Integer> speedingCount;
    public static final OutputTag<EnrichedTaxiSpeed> enrichedOutputTag = new OutputTag<EnrichedTaxiSpeed>("enriched-speed"){};
    private transient MetricsLogger metricsLogger;

    private static final double MIN_TIME_SECONDS = 2.0;
    private static final double MIN_DISTANCE_KM = 0.01;
    private static final double SPEED_LIMIT_KMPH = 50.0;
    private static final double MAX_REASONABLE_SPEED_KMPH = 300.0;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<TaxiLocation> descriptor = new ValueStateDescriptor<>("last-location",
                TypeInformation.of(TaxiLocation.class));
        lastLocationState = getRuntimeContext().getState(descriptor);

        ValueStateDescriptor<Integer> desc = new ValueStateDescriptor<>("speedingCount", Integer.class, 0);
        speedingCount = getRuntimeContext().getState(desc);

        metricsLogger = new MetricsLogger("flink-consumer", "calculateSpeedFunction", 5);
    }

    @Override
    public void processElement(TaxiLocation current, Context context, Collector<TaxiSpeed> out) throws Exception {

        long start = System.nanoTime();

        TaxiLocation last = lastLocationState.value();
        try {
            
            if (last != null) {
                double distanceKm = haversine(last.getLatitude(), last.getLongitude(),
                        current.getLatitude(), current.getLongitude());

                double timeDiffSeconds = (current.getTimestamp() - last.getTimestamp()) / 1000.0;

                if (timeDiffSeconds < MIN_TIME_SECONDS || distanceKm < MIN_DISTANCE_KM) {
                    System.out.println("Skipping record: timeDiff=" + timeDiffSeconds + "s, distance=" + distanceKm
                            + "km, TaxiID=" + current.getTaxiId());
                    return;
                }

                double speedKmph = Math.round((distanceKm / timeDiffSeconds) * 3600 * 100.0) / 100.0;

                if (speedKmph >= MAX_REASONABLE_SPEED_KMPH) {
                    System.out
                            .println("Unrealistic speed: " + speedKmph + " km/h for TaxiID=" + current.getTaxiId());
                    return;
                }

                boolean isSpeeding = speedKmph > SPEED_LIMIT_KMPH;

                if (isSpeeding) {
                    int count = speedingCount.value() + 1;
                    speedingCount.update(count);
                    RedisTaxiStatsWriter.logMessage("[Speeding Alert] Taxi Id:" + current.getTaxiId()
                            + " is speeding. Violation count: " + count + ".");
                }

                TaxiSpeed taxiSpeed = new TaxiSpeed();
                taxiSpeed.setTaxiId(current.getTaxiId());
                taxiSpeed.setTimestamp(current.getTimestamp());
                taxiSpeed.setSpeedKmph(speedKmph);
                taxiSpeed.setIsSpeeding(isSpeeding);

                out.collect(taxiSpeed);

                EnrichedTaxiSpeed enriched = new EnrichedTaxiSpeed(
                    current.getTaxiId(),
                    current.getTimestamp(),
                    current.getLatitude(),
                    current.getLongitude(),
                    speedKmph
                );

                enriched.setGridId(GridUtils.computeGridId(current.getLatitude(), current.getLongitude()));
                context.output(enrichedOutputTag, enriched);

            }

            if (current.isEndFlag()) {
                lastLocationState.clear();
                speedingCount.clear();
            } else {
                lastLocationState.update(current);
            }

        } catch (Exception e) {
            metricsLogger.recordError("Exception for TaxiID=" + current.getTaxiId() + ": " + e.getMessage());
        }
    }

    private double haversine(double lat1, double lon1, double lat2, double lon2) {
        final int R = 6371; // Radius of Earth in kilometers
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                        Math.sin(dLon / 2) * Math.sin(dLon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c;
    }
}
