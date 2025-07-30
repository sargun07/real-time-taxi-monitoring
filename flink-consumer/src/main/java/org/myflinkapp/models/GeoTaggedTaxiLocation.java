package org.myflinkapp.models;

import org.myflinkapp.models.TaxiLocation;

public class GeoTaggedTaxiLocation {
    private TaxiLocation location;
    private double distanceFromCenterKm;

    public GeoTaggedTaxiLocation(TaxiLocation location, double distanceFromCenterKm) {
        this.location = location;
        this.distanceFromCenterKm = distanceFromCenterKm;
    }

    public TaxiLocation getLocation() {
        return location;
    }

    public double getDistanceFromCenterKm() {
        return distanceFromCenterKm;
    }

    @Override
    public String toString() {
        return "GeoTaggedTaxiLocation{" +
               "taxiId='" + location.getTaxiId() + '\'' +
               ", timestamp=" + location.getTimestamp() +
               ", distanceFromCenterKm=" + distanceFromCenterKm +
               '}';
    }
}
