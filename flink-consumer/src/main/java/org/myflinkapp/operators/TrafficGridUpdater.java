package org.myflinkapp.operators;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.myflinkapp.models.EnrichedTaxiSpeed;
import org.myflinkapp.service.redis.RedisTaxiStatsWriter;

import java.util.Map;

public class TrafficGridUpdater extends KeyedProcessFunction<String, EnrichedTaxiSpeed, Void> {

    private transient MapState<String, Double> taxiSpeedMap;  
    private transient MapState<String, String> lastGridMap;   
    private transient ValueState<Long> lastUpdateTimerState;

    private static final double KM_PER_DEGREE_LAT = 111.32;
    private static final double KM_PER_DEGREE_LON = 85.23;
    private static final double CENTER_LAT = 39.9163;
    private static final double CENTER_LON = 116.3972;
    private static final double RADIUS_KM = 15.0;
    private static final double CELL_SIZE_KM = 0.5;

    private static final double LAT_DELTA = CELL_SIZE_KM / KM_PER_DEGREE_LAT;
    private static final double LON_DELTA = CELL_SIZE_KM / KM_PER_DEGREE_LON;
    private static final double MIN_LAT = CENTER_LAT - (RADIUS_KM / KM_PER_DEGREE_LAT);
    private static final double MIN_LON = CENTER_LON - (RADIUS_KM / KM_PER_DEGREE_LON);

    @Override
    public void open(Configuration parameters) {
        MapStateDescriptor<String, Double> taxiSpeedDesc = new MapStateDescriptor<>("taxiSpeeds", Types.STRING, Types.DOUBLE);
        MapStateDescriptor<String, String> lastGridDesc = new MapStateDescriptor<>("lastGridMap", Types.STRING, Types.STRING);
        ValueStateDescriptor<Long> timerDesc = new ValueStateDescriptor<>("lastTimer", Types.LONG);

        taxiSpeedMap = getRuntimeContext().getMapState(taxiSpeedDesc);
        lastGridMap = getRuntimeContext().getMapState(lastGridDesc);
        lastUpdateTimerState = getRuntimeContext().getState(timerDesc);
    }

    @Override
    public void processElement(EnrichedTaxiSpeed data, Context ctx, Collector<Void> out) throws Exception {
        String taxiId = data.getTaxiId();
        String currentGridId = data.getGridId();
        double speed = data.getSpeedKmph();

        // String lastGridId = lastGridMap.get(taxiId);

        // // If the taxi has moved from another grid, remove it from the old grid state
        // if (lastGridId != null && !lastGridId.equals(currentGridId)) {
        //     System.out.println("🚦 Taxi " + taxiId + " moved from " + lastGridId + " → " + currentGridId);

        // }

        // Update current grid’s taxi map
        taxiSpeedMap.put(taxiId, speed);
        lastGridMap.put(taxiId, currentGridId);

        // Register 10s timer if not already set
        if (lastUpdateTimerState.value() == null) {
            long triggerTime = ctx.timerService().currentProcessingTime() + Time.seconds(10).toMilliseconds();
            ctx.timerService().registerProcessingTimeTimer(triggerTime);
            lastUpdateTimerState.update(triggerTime);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Void> out) throws Exception {
        double totalSpeed = 0.0;
        int taxiCount = 0;

        for (Map.Entry<String, Double> entry : taxiSpeedMap.entries()) {
            totalSpeed += entry.getValue();
            taxiCount++;
        }

        if (taxiCount == 0) return;

        double avgSpeed = totalSpeed / taxiCount;

        String trafficStatus = avgSpeed < 5 && taxiCount >= 3 ? "CONGESTED"
                   : avgSpeed < 15 && taxiCount >= 3 ? "MODERATE"
                   : avgSpeed < 10 && taxiCount >= 2 ? "MODERATE"
                   : "FREE_FLOW";


        String[] parts = ctx.getCurrentKey().split("_");
        int latIndex = Integer.parseInt(parts[0]);
        int lonIndex = Integer.parseInt(parts[1]);

        double bottomLeftLat = MIN_LAT + latIndex * LAT_DELTA;
        double bottomLeftLon = MIN_LON + lonIndex * LON_DELTA;
        double topRightLat = bottomLeftLat + LAT_DELTA;
        double topRightLon = bottomLeftLon + LON_DELTA;

        long now = System.currentTimeMillis();
        RedisTaxiStatsWriter.writeTrafficGrid(ctx.getCurrentKey(), bottomLeftLat, bottomLeftLon, topRightLat, topRightLon, avgSpeed, taxiCount, trafficStatus, now);

        // Clear state for next window
        taxiSpeedMap.clear();
        lastUpdateTimerState.clear();
    }
}
