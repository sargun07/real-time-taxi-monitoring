package org.myflinkapp.models;

public class TaxiLocation {
    private String taxiId;
    private long timestamp;
    private double latitude;
    private double longitude;
    private boolean endFlag;
    private double producedTimestamp;

    public TaxiLocation() {
    }

    public TaxiLocation(String taxiId, long timestamp, double latitude, double longitude, boolean endFlag, double producedTimestamp) {
        this.taxiId = taxiId;
        this.timestamp = timestamp;
        this.latitude = latitude;
        this.longitude = longitude;
        this.endFlag = endFlag;
        this.producedTimestamp = producedTimestamp;
    }

    public String getTaxiId() {
        return taxiId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public boolean isEndFlag() {
        return endFlag;
    }

    public double getProducedTimestamp() {
        return producedTimestamp;
    }

    public void setTaxiId(String taxiId) {
        this.taxiId = taxiId;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public void setEndFlag(boolean endFlag) {
        this.endFlag = endFlag;
    }

    @Override
    public String toString() {
        return "TaxiLocation{" +
                "taxiId='" + taxiId + '\'' +
                ", timestamp=" + timestamp +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                ", endFlag=" + endFlag +
                '}';
    }
}
