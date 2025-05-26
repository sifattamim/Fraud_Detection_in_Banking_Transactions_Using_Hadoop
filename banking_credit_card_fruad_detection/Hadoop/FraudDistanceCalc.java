package com.pack;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

/**
 * Utility class to read zip code data from a CSV file and calculate distances
 * between two zip codes using their latitude and longitude.
 */
class ZipCodeData {
    double latitude;
    double longitude;
    String city;
    String stateName;
    String postId;

    public ZipCodeData(double latitude, double longitude, String city, String stateName, String postId) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.city = city;
        this.stateName = stateName;
        this.postId = postId;
    }
}

public class DistanceUtility {

    private static DistanceUtility instance = null;
    private final HashMap<String, ZipCodeData> zipCodeMap = new HashMap<>();

    // Singleton accessor
    public static DistanceUtility getInstance(String csvFilePath) throws IOException {
        if (instance == null) {
            instance = new DistanceUtility(csvFilePath);
        }
        return instance;
    }

    // Private constructor to load zip code data from CSV
    private DistanceUtility(String csvFilePath) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(",");

                String zipCode = fields[0];
                double latitude = Double.parseDouble(fields[1]);
                double longitude = Double.parseDouble(fields[2]);
                String city = fields[3];
                String stateName = fields[4];
                String postId = fields[5];

                ZipCodeData data = new ZipCodeData(latitude, longitude, city, stateName, postId);
                zipCodeMap.put(zipCode, data);
            }
        }
    }

    /**
     * Calculates distance (in kilometers) between two zip codes.
     */
    public double getDistanceBetweenZipCodes(String zipCode1, String zipCode2) {
        ZipCodeData z1 = zipCodeMap.get(zipCode1);
        ZipCodeData z2 = zipCodeMap.get(zipCode2);

        if (z1 == null || z2 == null) {
            throw new IllegalArgumentException("Invalid zip code(s): " + zipCode1 + " or " + zipCode2);
        }

        return calculateDistance(z1.latitude, z1.longitude, z2.latitude, z2.longitude);
    }

    // Haversine formula for calculating distance between 2 lat/lon points
    private double calculateDistance(double lat1, double lon1, double lat2, double lon2) {
        double theta = lon1 - lon2;
        double dist = Math.sin(degToRad(lat1)) * Math.sin(degToRad(lat2)) +
                      Math.cos(degToRad(lat1)) * Math.cos(degToRad(lat2)) * Math.cos(degToRad(theta));
        dist = Math.acos(dist);
        dist = radToDeg(dist);
        dist = dist * 60 * 1.1515; // miles
        return dist * 1.609344;    // convert miles to kilometers
    }

    private double degToRad(double degree) {
        return degree * Math.PI / 180.0;
    }

    private double radToDeg(double radian) {
        return radian * 180.0 / Math.PI;
    }
}
