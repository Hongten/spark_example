/**
 * Spark Example  
 * Mail: hongtenzone@foxmail.com 
 * Blog: http://www.cnblogs.com/hongten
 */
package com.hongten.spark.example.streaming;

import java.io.Serializable;

import org.apache.log4j.Logger;

import com.hongten.spark.example.streaming.util.RealTimeDataGenerateUtils;

/**
 * 需求：使用SparkStreaming，并且结合Kafka，获取实时道路交通拥堵情况信息。<br>
 * 
 * 目的： 对监控点平均车速进行监控，可以实时获取交通拥堵情况信息。相关部门可以对交通拥堵情况采取措施。<br>
 *     e.g.1.通过广播方式，让司机改道。<br>
 *         2.通过实时交通拥堵情况数据，反映在一些APP上面，形成实时交通拥堵情况地图，方便用户查询。<br>
 * 
 * @author Hongten
 * @created 22 Jan, 2019
 */
public class RealTimeVehicleSpeedMonitorMain implements Serializable {

	private static final long serialVersionUID = 1L;

	static final Logger logger = Logger.getLogger(RealTimeVehicleSpeedMonitorMain.class);

	public static void main(String[] args) {
		logger.info("Begin Task");
		RealTimeVehicleSpeedMonitorMain realTimeVehicleSpeedMonitor = new RealTimeVehicleSpeedMonitorMain();
		realTimeVehicleSpeedMonitor.processRealTimeVehicleSpeedMonitor();
	}

	private void processRealTimeVehicleSpeedMonitor() {
		generateDataToKafka();
		
		
		
		
	}

	/**
	 * 向Kafka中产生实时Vehicle Log数据
	 */
	private void generateDataToKafka() {
		logger.info("Begin Generate Data to Kafka");
		RealTimeDataGenerateUtils.generateDataToKafka();
	}

}
