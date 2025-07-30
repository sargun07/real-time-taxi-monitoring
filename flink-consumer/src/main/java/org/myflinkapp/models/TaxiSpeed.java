package org.myflinkapp.models;

public class TaxiSpeed {

    private String taxiId;
    private long timestamp;
    private double speedKmph;
    private boolean isSpeeding;

    // No-arg constructor (required by Flink)
    public TaxiSpeed() {}

    // All-args constructor (optional, convenient)
    public TaxiSpeed(String taxiId, long timestamp, double speedKmph, boolean isSpeeding) {
        this.taxiId = taxiId;
        this.timestamp = timestamp;
        this.speedKmph = speedKmph;
        this.isSpeeding = isSpeeding;
    }

    // Getters
    public String getTaxiId() {
        return taxiId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public double getSpeedKmph() {
        return speedKmph;
    }

    public boolean isSpeeding() {
        return isSpeeding;
    }

    // Setters
    public void setTaxiId(String taxiId) {
        this.taxiId = taxiId;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setSpeedKmph(double speedKmph) {
        this.speedKmph = speedKmph;
    }

    public void setIsSpeeding(boolean isSpeeding) {
        this.isSpeeding = isSpeeding;
    }
}
