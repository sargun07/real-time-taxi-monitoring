package org.myflinkapp.operators;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.myflinkapp.models.TaxiLocation;
import org.myflinkapp.models.GeoTaggedTaxiLocation;

public class AddDistanceFromCenterFunction extends RichFlatMapFunction<TaxiLocation, GeoTaggedTaxiLocation> {

    private static final double CENTER_LAT = 39.9163;
    private static final double CENTER_LON = 116.3972;
    private transient ValueState<Long> lastTimestampState;

    
    @Override
    public void open(Configuration parameters) {

        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>(
            "lastTimestamp", Long.class
        );
        lastTimestampState = getRuntimeContext().getState(descriptor);
    }


    @Override
    public void flatMap(TaxiLocation location, Collector<GeoTaggedTaxiLocation> out) throws Exception {
        Long lastTimestamp = lastTimestampState.value();
        long currentTimestamp = location.getTimestamp();
        String taxiId = location.getTaxiId();

        if (lastTimestamp != null && currentTimestamp < lastTimestamp) {
            System.out.println("Timestamp out-of-order for TaxiID=" + taxiId + " | Current=" + currentTimestamp + " < Last=" + lastTimestamp);
            return;  
        }

        double distance = haversine(CENTER_LAT, CENTER_LON, location.getLatitude(), location.getLongitude());

        lastTimestampState.update(currentTimestamp);

        out.collect(new GeoTaggedTaxiLocation(location, distance));
    }


    private double haversine(double lat1, double lon1, double lat2, double lon2) {
        final int R = 6371; // Radius of the earth in km
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                   Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                   Math.sin(dLon / 2) * Math.sin(dLon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c;
    }
}
