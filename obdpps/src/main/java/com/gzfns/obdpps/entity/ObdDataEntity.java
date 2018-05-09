package com.gzfns.obdpps.entity;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Date;

public class ObdDataEntity implements Serializable {

    @JsonProperty(value = "Id")
	private Long id;

    @JsonProperty(value = "Imei")
    private String imei;

    @JsonProperty(value = "ReportTime")
    private Date reportTime;

    @JsonProperty(value = "ReceiveTime")
    private Date receiveTime;

    @JsonProperty(value = "Latitude")
    private String latitude;

    @JsonProperty(value = "Longitude")
    private String longitude;

    @JsonProperty(value = "BLatitude")
    private String bLatitude;

    @JsonProperty(value = "BLongitude")
    private String bLongitude;

    @JsonProperty(value = "TotalMileage")
    private float totalMileage;

    @JsonProperty(value = "Speed")
    private float speed;

    @JsonProperty(value = "Orientation")
    private float orientation;

    @JsonProperty(value = "IsValid")
    private int isValid;

    @JsonProperty(value = "LocationType")
    private int locationType;

    @JsonProperty(value = "ExternalVoltage")
    private Long externalVoltage;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getImei() {
        return imei;
    }

    public void setImei(String imei) {
        this.imei = imei;
    }

    public Date getReportTime() {
        return reportTime;
    }

    public void setReportTime(Date reportTime) {
        this.reportTime = reportTime;
    }

    public Date getReceiveTime() {
        return receiveTime;
    }

    public void setReceiveTime(Date receiveTime) {
        this.receiveTime = receiveTime;
    }

    public String getLatitude() {
        return latitude;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }

    public String getLongitude() {
        return longitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }

    public String getbLatitude() {
        return bLatitude;
    }

    public void setbLatitude(String bLatitude) {
        this.bLatitude = bLatitude;
    }

    public String getbLongitude() {
        return bLongitude;
    }

    public void setbLongitude(String bLongitude) {
        this.bLongitude = bLongitude;
    }

    public float getTotalMileage() {
        return totalMileage;
    }

    public void setTotalMileage(float totalMileage) {
        this.totalMileage = totalMileage;
    }

    public float getSpeed() {
        return speed;
    }

    public void setSpeed(float speed) {
        this.speed = speed;
    }

    public float getOrientation() {
        return orientation;
    }

    public void setOrientation(float orientation) {
        this.orientation = orientation;
    }

    public int getIsValid() {
        return isValid;
    }

    public void setIsValid(int isValid) {
        this.isValid = isValid;
    }

    public int getLocationType() {
        return locationType;
    }

    public void setLocationType(int locationType) {
        this.locationType = locationType;
    }

    public Long getExternalVoltage() {
        return externalVoltage;
    }

    public void setExternalVoltage(Long externalVoltage) {
        this.externalVoltage = externalVoltage;
    }
}
