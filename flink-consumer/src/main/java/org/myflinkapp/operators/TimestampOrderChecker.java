package org.myflinkapp.operators;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.myflinkapp.models.TaxiLocation;

public class TimestampOrderChecker extends RichFlatMapFunction<TaxiLocation, TaxiLocation> {

    private transient ValueState<Long> lastTimestampState;

    private static int inOrderCount = 0;
    private static int outOfOrderCount = 0;

    @Override
    public void open(Configuration parameters) {
        lastTimestampState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("lastTimestamp", Long.class)
        );
    }

    @Override
    public void flatMap(TaxiLocation event, Collector<TaxiLocation> out) throws Exception {
        Long lastTimestamp = lastTimestampState.value();

        if (lastTimestamp == null || event.getTimestamp() >= lastTimestamp) {
            inOrderCount++;
        } else {
            outOfOrderCount++;
        }
        
        int total = inOrderCount + outOfOrderCount;
        double inOrderPercent = (inOrderCount * 100.0) / total;
        double outOfOrderPercent = (outOfOrderCount * 100.0) / total;

        System.out.printf("✅ In-order: %d (%.2f%%) | ❌ Out-of-order: %d (%.2f%%)%n",
        inOrderCount, inOrderPercent, outOfOrderCount, outOfOrderPercent);

        lastTimestampState.update(event.getTimestamp());
        out.collect(event);  
    }

}
