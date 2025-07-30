package org.myflinkapp.models;

public class TaxiStats {
    private String taxiId;
    private double totalDistanceKm;
    private long totalTimeSeconds;
    private double averageSpeedKmph;
    private TaxiLocation latestLocation;
    private double producedTimestamp;

    public TaxiStats() {
    }

    public TaxiStats(String taxiId, double totalDistanceKm, long totalTimeSeconds, double averageSpeedKmph) {
        this.taxiId = taxiId;
        this.totalDistanceKm = totalDistanceKm;
        this.totalTimeSeconds = totalTimeSeconds;
        this.averageSpeedKmph = averageSpeedKmph;
    }

    public String getTaxiId() {
        return taxiId;
    }

    public void setTaxiId(String taxiId) {
        this.taxiId = taxiId;
    }

    public double getTotalDistanceKm() {
        return totalDistanceKm;
    }

    public void setTotalDistanceKm(double totalDistanceKm) {
        this.totalDistanceKm = totalDistanceKm;
    }

    public long getTotalTimeSeconds() {
        return totalTimeSeconds;
    }

    public void setTotalTimeSeconds(long totalTimeSeconds) {
        this.totalTimeSeconds = totalTimeSeconds;
    }

    public void setProducedTimestamp(double producedTimestamp) {
        this.producedTimestamp = producedTimestamp;
    }

    public double getProducedTimestamp() {
        return producedTimestamp;
    }

    public double getAverageSpeedKmph() {
        return averageSpeedKmph;
    }

    public void setAverageSpeedKmph(double averageSpeedKmph) {
        this.averageSpeedKmph = averageSpeedKmph;
    }

    public TaxiLocation getLatestLocation() {
        return latestLocation;
    }

    public void setLatestLocation(TaxiLocation latestLocation) {
        this.latestLocation = latestLocation;
    }
}
