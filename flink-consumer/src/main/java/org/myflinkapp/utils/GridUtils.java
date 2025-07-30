package org.myflinkapp.utils;

public class GridUtils {

    // Constants
    private static final double KM_PER_DEG_LAT = 111.32;
    private static final double KM_PER_DEG_LON = 85.23;
    private static final double CENTER_LAT = 39.9163;
    private static final double CENTER_LON = 116.3972;
    private static final double RADIUS_KM = 15.0;
    private static final double CELL_SIZE_KM = 0.5;

    // Derived constants
    private static final double LAT_DELTA = CELL_SIZE_KM / KM_PER_DEG_LAT;
    private static final double LON_DELTA = CELL_SIZE_KM / KM_PER_DEG_LON;
    private static final double MIN_LAT = CENTER_LAT - (RADIUS_KM / KM_PER_DEG_LAT);
    private static final double MIN_LON = CENTER_LON - (RADIUS_KM / KM_PER_DEG_LON);

    public static String computeGridId(double latitude, double longitude) {
        int latIndex = (int) ((latitude - MIN_LAT) / LAT_DELTA);
        int lonIndex = (int) ((longitude - MIN_LON) / LON_DELTA);
        return latIndex + "_" + lonIndex;
    }

    public static double[] getGridBounds(String gridId) {
        String[] parts = gridId.split("_");
        int latIndex = Integer.parseInt(parts[0]);
        int lonIndex = Integer.parseInt(parts[1]);

        double bottomLeftLat = MIN_LAT + latIndex * LAT_DELTA;
        double bottomLeftLon = MIN_LON + lonIndex * LON_DELTA;
        double topRightLat = bottomLeftLat + LAT_DELTA;
        double topRightLon = bottomLeftLon + LON_DELTA;

        return new double[]{bottomLeftLat, bottomLeftLon, topRightLat, topRightLon};
    }
}
