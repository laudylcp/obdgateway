package com.gzfns.obdpps.entity;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Date;

public class DeviceAlarmEntity implements Serializable {

    @JsonProperty(value = "Id")
    private Long id;

    @JsonProperty(value = "Imei")
    private String imei;

    @JsonProperty(value = "AlarmCode")
    private String alarmCode;

    @JsonProperty(value = "AlarmTime")
    private Date alarmTime;

    @JsonProperty(value = "AlarmThresholdValue")
    private String alarmThresholdValue;

    @JsonProperty(value = "AlarmStatus")
    private Short alarmStatus;

    @JsonProperty(value = "AlarmRestoreTime")
    private Date alarmRestoreTime;

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

    public String getAlarmCode() {
        return alarmCode;
    }

    public void setAlarmCode(String alarmCode) {
        this.alarmCode = alarmCode;
    }

    public Date getAlarmTime() {
        return alarmTime;
    }

    public void setAlarmTime(Date alarmTime) {
        this.alarmTime = alarmTime;
    }

    public String getAlarmThresholdValue() {
        return alarmThresholdValue;
    }

    public void setAlarmThresholdValue(String alarmThresholdValue) {
        this.alarmThresholdValue = alarmThresholdValue;
    }

    public Short getAlarmStatus() {
        return alarmStatus;
    }

    public void setAlarmStatus(Short alarmStatus) {
        this.alarmStatus = alarmStatus;
    }

    public Date getAlarmRestoreTime() {
        return alarmRestoreTime;
    }

    public void setAlarmRestoreTime(Date alarmRestoreTime) {
        this.alarmRestoreTime = alarmRestoreTime;
    }
}
