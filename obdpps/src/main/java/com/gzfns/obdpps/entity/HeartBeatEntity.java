package com.gzfns.obdpps.entity;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Date;

public class HeartBeatEntity implements Serializable {

	@JsonProperty(value = "Imei")
	private String imei;

	@JsonProperty(value = "HeartbeatTime")
	private Date heartbeatTime;

	@JsonProperty(value = "HadOfflineAlarm")
	private int hadOfflineAlarm;

	public String getImei() {
		return imei;
	}

	public void setImei(String imei) {
		this.imei = imei;
	}

	public Date getHeartbeatTime() {
		return heartbeatTime;
	}

	public void setHeartbeatTime(Date heartbeatTime) {
		this.heartbeatTime = heartbeatTime;
	}

	public int getHadOfflineAlarm() {
		return hadOfflineAlarm;
	}

	public void setHadOfflineAlarm(int hadOfflineAlarm) {
		this.hadOfflineAlarm = hadOfflineAlarm;
	}
}
