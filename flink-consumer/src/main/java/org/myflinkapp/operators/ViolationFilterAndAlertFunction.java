package org.myflinkapp.operators;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.myflinkapp.models.GeoTaggedTaxiLocation;
import org.myflinkapp.models.TaxiLocation;
import org.myflinkapp.service.redis.RedisTaxiStatsWriter;
import org.myflinkapp.service.metrics.MetricsLogger;

public class ViolationFilterAndAlertFunction extends RichFlatMapFunction<GeoTaggedTaxiLocation, TaxiLocation> {

    private transient ValueState<Boolean> warnedForArea;
    private transient ValueState<Integer> areaViolationCount;

    private transient MetricsLogger metricsLogger;

    @Override
    public void open(Configuration parameters) {
        warnedForArea = getRuntimeContext().getState(
                new ValueStateDescriptor<>("warnedForArea", Boolean.class));

        areaViolationCount = getRuntimeContext().getState(
                new ValueStateDescriptor<>("areaViolationCount", Integer.class, 0));

        metricsLogger = new MetricsLogger("flink-consumer", "violationFilterAndAlert", 5);
    }

    @Override
    public void flatMap(GeoTaggedTaxiLocation geoTagged, Collector<TaxiLocation> out) throws Exception {
        long start = System.nanoTime();

        try {
            double distance = geoTagged.getDistanceFromCenterKm();
            TaxiLocation location = geoTagged.getLocation();
            String taxiId = location.getTaxiId();

            Boolean warned = warnedForArea.value();

            if (distance > 15.0) {
                int count = areaViolationCount.value() + 1;
                areaViolationCount.update(count);
                System.out.println("Discarded: TaxiID=" + taxiId + " is " + String.format("%.2f", distance)
                        + " km from Forbidden City");
                RedisTaxiStatsWriter.markRemoved(taxiId, count);
                RedisTaxiStatsWriter.resetDistance(taxiId);
                return;
            }

            if (distance <= 10.0 && warned != null && warned) {
                System.out.println("TaxiID=" + taxiId + " returned inside the 10 km zone. Resetting warning.");
                warnedForArea.update(false);
                RedisTaxiStatsWriter.writeTenKmStatus(taxiId, false);
            }

            if (distance > 10.0 && (warned == null || !warned)) {
                System.out.println("Warning: TaxiID=" + taxiId + " is " + String.format("%.2f", distance)
                        + " km from Forbidden City (over 10 km)");
                warnedForArea.update(true);
                RedisTaxiStatsWriter.writeTenKmStatus(taxiId, true);

                RedisTaxiStatsWriter.logMessage("[Warning: Area Violation] Taxi Id:" + taxiId + " exceeded 10km zone.");
            }

            out.collect(location);


        } catch (Exception e) {
            metricsLogger
                    .recordError("Exception for TaxiID=" + geoTagged.getLocation().getTaxiId() + ": " + e.getMessage());
            throw e;
        }
    }
}
