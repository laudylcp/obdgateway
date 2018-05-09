package com.gzfns.obdpps.util;

public class Gps
{
    public Gps(double lat, double lon)
    {
        this.Lat = lat;
        this.Lon = lon;
    }

    private double Lat;

    private double Lon;

    public double getLat() {
        return Lat;
    }

    public void setLat(double lat) {
        Lat = lat;
    }

    public double getLon() {
        return Lon;
    }

    public void setLon(double lon) {
        Lon = lon;
    }
}
