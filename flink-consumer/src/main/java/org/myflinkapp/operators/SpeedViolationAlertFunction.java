package org.myflinkapp.operators;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.myflinkapp.models.TaxiSpeed;
import org.myflinkapp.service.metrics.MetricsLogger;

public class SpeedViolationAlertFunction extends RichFlatMapFunction<TaxiSpeed, String> {

    private transient MetricsLogger metricsLogger;

    @Override
    public void open(Configuration parameters) {
        metricsLogger = new MetricsLogger("flink-consumer", "speedViolationAlertFunction", 5);
    }

    @Override
    public void flatMap(TaxiSpeed speed, Collector<String> out) throws Exception {
        long start = System.nanoTime();

        try {
            if (speed.isSpeeding()) {
                out.collect("Speeding Alert! TaxiID=" + speed.getTaxiId() + " | Speed=" + speed.getSpeedKmph()
                        + " km/h");
            }
        } catch (Exception e) {
            metricsLogger.recordError("Exception for TaxiID=" + speed.getTaxiId() + ": " + e.getMessage());
            throw e;
        }
    }
}
