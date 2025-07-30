package org.myflinkapp.utils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.ZoneOffset;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.myflinkapp.models.TaxiLocation;

import java.io.IOException;

public class TaxiLocationDeserialization implements DeserializationSchema<TaxiLocation> {

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    @Override
    public TaxiLocation deserialize(byte[] message) throws IOException {
        try {
            
            String value = new String(message);
            
            if (value.startsWith("taxi_id")) return null;

            String[] fields = value.split(",");

            String taxiId = fields[0];
            LocalDateTime dateTime = LocalDateTime.parse(fields[1], formatter);
            long timestamp = dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
            double longitude = Double.parseDouble(fields[2]);
            double latitude = Double.parseDouble(fields[3]);
            boolean endFlag = fields[4].trim().equals("1") || fields[4].trim().equalsIgnoreCase("true");
            double producedTimestamp = Double.parseDouble(fields[5]);

            return new TaxiLocation(taxiId, timestamp, latitude, longitude, endFlag, producedTimestamp);
        } catch (Exception e) {
            System.err.println("Failed to parse message: " + new String(message));
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(TaxiLocation nextElement) {
        return false;
    }

    @Override
    public TypeInformation<TaxiLocation> getProducedType() {
        return TypeInformation.of(TaxiLocation.class);
    }
}
