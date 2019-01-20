/**
 * Spark Example  
 * Mail: hongtenzone@foxmail.com 
 * Blog: http://www.cnblogs.com/hongten
 */
package com.hongten.spark.example.traffic.datagenerate.vo;

import java.io.Serializable;

/**
 * @author Hongten
 * @created 20 Jan, 2019
 */
public class VehicleLogVO implements Serializable {

	private static final long serialVersionUID = -7858585505355892518L;

	private String dataTime;
	private String vehiclePlate;
	private int vechicleSpeed;
	private String roadId;
	private String monitorId;
	private String cameraId;

	public VehicleLogVO(String dataTime, String vehiclePlate, int vechicleSpeed, String roadId, String monitorId, String cameraId) {
		super();
		this.dataTime = dataTime;
		this.vehiclePlate = vehiclePlate;
		this.vechicleSpeed = vechicleSpeed;
		this.roadId = roadId;
		this.monitorId = monitorId;
		this.cameraId = cameraId;
	}

	public VehicleLogVO(String roadId, String monitorId, String cameraId) {
		super();
		this.roadId = roadId;
		this.monitorId = monitorId;
		this.cameraId = cameraId;
	}

	public String getDataTime() {
		return dataTime;
	}

	public void setDataTime(String dataTime) {
		this.dataTime = dataTime;
	}

	public String getVehiclePlate() {
		return vehiclePlate;
	}

	public void setVehiclePlate(String vehiclePlate) {
		this.vehiclePlate = vehiclePlate;
	}

	public int getVechicleSpeed() {
		return vechicleSpeed;
	}

	public void setVechicleSpeed(int vechicleSpeed) {
		this.vechicleSpeed = vechicleSpeed;
	}

	public String getRoadId() {
		return roadId;
	}

	public void setRoadId(String roadId) {
		this.roadId = roadId;
	}

	public String getMonitorId() {
		return monitorId;
	}

	public void setMonitorId(String monitorId) {
		this.monitorId = monitorId;
	}

	public String getCameraId() {
		return cameraId;
	}

	public void setCameraId(String cameraId) {
		this.cameraId = cameraId;
	}

}
