package com.gzfns.obdpps.entity;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Date;

public class DeviceVehicleEntity implements Serializable {
	@JsonProperty(value = "Imei")
	private String imei;

	@JsonProperty(value = "Vin")
	private String vin;

	@JsonProperty(value = "LinkedTime")
	private Date linkedTime;

	public String getImei() {
		return imei;
	}

	public void setImei(String imei) {
		this.imei = imei;
	}

	public String getVin() {
		return vin;
	}

	public void setVin(String vin) {
		this.vin = vin;
	}

	public Date getLinkedTime() {
		return linkedTime;
	}

	public void setLinkedTime(Date linkedTime) {
		this.linkedTime = linkedTime;
	}
}
