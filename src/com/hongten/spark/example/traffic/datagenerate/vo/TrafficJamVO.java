/**
 * Spark Example  
 * Mail: hongtenzone@foxmail.com 
 * Blog: http://www.cnblogs.com/hongten
 */
package com.hongten.spark.example.traffic.datagenerate.vo;

import java.io.Serializable;

/**
 * 这是一个比较类。
 * 
 * 首先比较的是dateTime， 它的值是从VehicleLogVO.dateTime转化而来。
 * 再比较monitorId，它的值是从VehicleLogVO.monitorId转化而来。
 * 
 * @see com.hongten.spark.example.traffic.datagenerate.vo.VehicleLogVO
 * 
 * @author Hongten
 * @created 22 Jan, 2019
 */
public class TrafficJamVO implements Comparable<TrafficJamVO>, Serializable {

	private static final long serialVersionUID = 1L;

	private String dateTimeStr;
	private int dateTime;
	private int monitorId;
	private boolean ascending;
	private int ascendingIndicator;

	@Override
	public int compareTo(TrafficJamVO that) {
		// 判断按照什么方式进行排序
		if (this.ascending) {
			// 升序排序
			this.ascendingIndicator = -1;
		} else {
			// 降序排序
			this.ascendingIndicator = 1;
		}
		// 先比较dateTime
		if (this.dateTime == that.dateTime) {
			// 再比较monitorId
			return (this.monitorId - that.monitorId) * this.ascendingIndicator;
		}
		return (this.dateTime - that.dateTime) * this.ascendingIndicator;
	}

	public TrafficJamVO(String dateTimeStr, int dateTime, int monitorId, boolean ascending) {
		super();
		this.dateTimeStr = dateTimeStr;
		this.dateTime = dateTime;
		this.monitorId = monitorId;
		this.ascending = ascending;
	}

	public int getDateTime() {
		return dateTime;
	}

	public void setDateTime(int dateTime) {
		this.dateTime = dateTime;
	}

	public int getMonitorId() {
		return monitorId;
	}

	public void setMonitorId(int monitorId) {
		this.monitorId = monitorId;
	}

	public boolean isAscending() {
		return ascending;
	}

	public void setAscending(boolean ascending) {
		this.ascending = ascending;
	}

	public int getAscendingIndicator() {
		return ascendingIndicator;
	}

	public void setAscendingIndicator(int ascendingIndicator) {
		this.ascendingIndicator = ascendingIndicator;
	}

	public String getDateTimeStr() {
		return dateTimeStr;
	}

	public void setDateTimeStr(String dateTimeStr) {
		this.dateTimeStr = dateTimeStr;
	}
	

}
