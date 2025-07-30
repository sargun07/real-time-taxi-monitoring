package org.myflinkapp.models;

public class EnrichedTaxiSpeed {

    private String taxiId;
    private long timestamp;

    private double latitude;
    private double longitude;

    private double speedKmph;
    private String gridId;
  

    public EnrichedTaxiSpeed() {}

    public EnrichedTaxiSpeed(String taxiId, long timestamp, double latitude, double longitude, double speedKmph) {
        this.taxiId = taxiId;
        this.timestamp = timestamp;
        this.latitude = latitude;
        this.longitude = longitude;
        this.speedKmph = speedKmph;
    }


    public String getTaxiId() {
        return taxiId;
    }

    public void setTaxiId(String taxiId) {
        this.taxiId = taxiId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getSpeedKmph() {
        return speedKmph;
    }

    public void setSpeedKmph(double speedKmph) {
        this.speedKmph = speedKmph;
    }

    public String getGridId() {
        return gridId;
    }

    public void setGridId(String gridId) {
        this.gridId = gridId;
    }

    @Override
    public String toString() {
        return "EnrichedTaxiSpeed{" +
                "taxiId='" + taxiId + '\'' +
                ", timestamp=" + timestamp +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                ", speedKmph=" + speedKmph +
                '}';
    }
}
