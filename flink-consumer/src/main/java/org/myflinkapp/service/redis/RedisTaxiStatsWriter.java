package org.myflinkapp.service.redis;

import org.myflinkapp.models.*;

public class RedisTaxiStatsWriter {

    public static void writeStats(TaxiStats stats) {
        RedisCacheManager redis = RedisCacheManager.getInstance();
        TaxiLocation location = stats.getLatestLocation();
        String taxiId = stats.getTaxiId();
        redis.updateStats(
            taxiId,
            location.getLatitude(),
            location.getLongitude(),
            location.getTimestamp(),
            stats.getAverageSpeedKmph(),
            stats.getTotalDistanceKm()
        );

        if (stats.getLatestLocation().isEndFlag()) {
            redis.markTaxiInactive(taxiId);
        } else {
            String currentStatus = getStatus(taxiId);  
            if (!"halted".equals(currentStatus)) {
                redis.markTaxiActive(taxiId);
            } 
        }
    }

    public static void writeSpeedingStatus(TaxiSpeed speed) {
        RedisCacheManager redis = RedisCacheManager.getInstance();
        redis.updateCurrentSpeed(speed.getTaxiId(), speed.getSpeedKmph());
        redis.updateSpeedingStatus(speed.getTaxiId(), speed.isSpeeding(), speed.getSpeedKmph());
    }

    public static void writeTenKmStatus(String taxiId, boolean crossed) {
        RedisCacheManager.getInstance().setTenKmCrossed(taxiId, crossed);
    }

    public static void logMessage(String message) {
        RedisCacheManager.getInstance().pushLog(message);
    }

    public static void writeTotalDistance(String taxiId, double totalDistance) {
        RedisCacheManager redis = RedisCacheManager.getInstance();
        redis.updateTotalDistance(taxiId, totalDistance);

    }

    public static void resetDistance(String taxiId) {
        RedisCacheManager redis = RedisCacheManager.getInstance();
        redis.resetTotalDistance(taxiId);
    }

    public static void markRemoved(String taxiId, int areaViolationCount) {
        RedisCacheManager redis = RedisCacheManager.getInstance();
        redis.markTaxiRemoved(taxiId, areaViolationCount);
    }

    public static String getStatus(String taxiId) {
        RedisCacheManager redis = RedisCacheManager.getInstance();
        return redis.getTaxiStatus(taxiId);
    }

    public static void markHalted(String taxiId, boolean isHalted) {
        RedisCacheManager redis = RedisCacheManager.getInstance();
        redis.setTaxiHaltedStatus(taxiId, isHalted);
    }

    public static void writeTrafficGrid(String gridId, double bottomLeftLat, double bottomLeftLon, double topRightLat, double topRightLon, double avgSpeed, int taxiCount, String trafficStatus, long timestamp) {
        RedisCacheManager redis = RedisCacheManager.getInstance();
        String key = "grid:" + gridId;
        redis.updateTrafficgrid(key, bottomLeftLat, bottomLeftLon, topRightLat, topRightLon, avgSpeed, taxiCount, trafficStatus, timestamp);   
    }
}
