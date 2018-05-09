package com.gzfns.obdpps.util;

public class PositionUtils {

    private static double a = 6378245.0;

    private static double ee = 0.00669342162296594323;

    public static Gps WGS84_To_GCJ02(double lat, double lon)
    {
        return transform(lat, lon);
    }

    public static Gps GCJ02_To_WGS84(double lat, double lon)
    {
        Gps gps = transform(lat, lon);
        double longitude = lon * 2 - gps.getLon();
        double latitude = lat * 2 - gps.getLat();
        return new Gps(latitude, longitude);
    }

    public static Gps GCJ02_To_BD09(double gg_lat, double gg_lon)
    {
        double x = gg_lon, y = gg_lat;
        double z = Math.sqrt(x * x + y * y) + 0.00002 * Math.sin(y * Math.PI);
        double theta = Math.atan2(y, x) + 0.000003 * Math.cos(x * Math.PI);
        double bd_lon = z * Math.cos(theta) + 0.0065;
        double bd_lat = z * Math.sin(theta) + 0.006;
        return new Gps(bd_lat, bd_lon);
    }

    public static Gps BD09_To_GCJ02(double bd_lat, double bd_lon)
    {
        double x = bd_lon - 0.0065, y = bd_lat - 0.006;
        double z = Math.sqrt(x * x + y * y) - 0.00002 * Math.sin(y * Math.PI);
        double theta = Math.atan2(y, x) - 0.000003 * Math.cos(x * Math.PI);
        double gg_lon = z * Math.cos(theta);
        double gg_lat = z * Math.sin(theta);
        return new Gps(gg_lat, gg_lon);
    }

    public static Gps BD09_To_WGS84(double bd_lat, double bd_lon)
    {
        Gps gcj02 = PositionUtils.BD09_To_GCJ02(bd_lat, bd_lon);
        Gps map84 = PositionUtils.GCJ02_To_WGS84(gcj02.getLat(), gcj02.getLon());
        return map84;
    }

    public static Gps WGS84_To_BD09(double lat, double lon)
    {
        Gps gcj02 = PositionUtils.WGS84_To_GCJ02(lat, lon);
        Gps bd09 = PositionUtils.GCJ02_To_BD09(gcj02.getLat(), gcj02.getLon());
        return bd09;
    }

    public static boolean outOfChina(double lat, double lon)
    {
        if (lon < 72.004 || lon > 137.8347)
            return true;
        if (lat < 0.8293 || lat > 55.8271)
            return true;
        return false;
    }

    public static double transformLat(double x, double y)
    {
        double ret = -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y + 0.2 * Math.sqrt(Math.abs(x));
        ret += (20.0 * Math.sin(6.0 * x * Math.PI) + 20.0 * Math.sin(2.0 * x * Math.PI)) * 2.0 / 3.0;
        ret += (20.0 * Math.sin(y * Math.PI) + 40.0 * Math.sin(y / 3.0 * Math.PI)) * 2.0 / 3.0;
        ret += (160.0 * Math.sin(y / 12.0 * Math.PI) + 320 * Math.sin(y * Math.PI / 30.0)) * 2.0 / 3.0;
        return ret;
    }

    public static double transformLon(double x, double y)
    {
        double ret = 300.0 + x + 2.0 * y + 0.1 * x * x + 0.1 * x * y + 0.1 * Math.sqrt(Math.abs(x));
        ret += (20.0 * Math.sin(6.0 * x * Math.PI) + 20.0 * Math.sin(2.0 * x * Math.PI)) * 2.0 / 3.0;
        ret += (20.0 * Math.sin(x * Math.PI) + 40.0 * Math.sin(x / 3.0 * Math.PI)) * 2.0 / 3.0;
        ret += (150.0 * Math.sin(x / 12.0 * Math.PI) + 300.0 * Math.sin(x / 30.0 * Math.PI)) * 2.0 / 3.0;
        return ret;
    }

    public static Gps transform(double lat, double lon)
    {
        if (outOfChina(lat, lon))
        {
            return new Gps(lat, lon);
        }
        double dLat = transformLat(lon - 105.0, lat - 35.0);
        double dLon = transformLon(lon - 105.0, lat - 35.0);
        double radLat = lat / 180.0 * Math.PI;
        double magic = Math.sin(radLat);
        magic = 1 - ee * magic * magic;
        double sqrtMagic = Math.sqrt(magic);
        dLat = (dLat * 180.0) / ((a * (1 - ee)) / (magic * sqrtMagic) * Math.PI);
        dLon = (dLon * 180.0) / (a / sqrtMagic * Math.cos(radLat) * Math.PI);
        double mgLat = lat + dLat;
        double mgLon = lon + dLon;
        return new Gps(mgLat, mgLon);
    }    
}


