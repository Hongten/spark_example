/**
 * Spark Example  
 * Mail: hongtenzone@foxmail.com 
 * Blog: http://www.cnblogs.com/hongten
 */
package com.hongten.spark.example.traffic;

import java.io.Serializable;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import com.hongten.spark.example.traffic.datagenerate.Common;
import com.hongten.spark.example.traffic.datagenerate.util.DataLoadUtils;

/**
 * 需求: 在所有监控点里面，超速（max speed: 250）车辆最多的10个监控点是什么？
 * 
 * @author Hongten
 * @created 20 Jan, 2019
 */
public class Top10OverMaxSpeedMonitorMain implements Serializable{

	private static final long serialVersionUID = 1L;

	static final Logger logger = Logger.getLogger(Top10OverMaxSpeedMonitorMain.class);
	
	public static void main(String[] args) {
		long begin = System.currentTimeMillis();
		Top10OverMaxSpeedMonitorMain top10OverMaxSpeedMonitorMain = new Top10OverMaxSpeedMonitorMain();
		top10OverMaxSpeedMonitorMain.processTop10OverMaxSpeedMonitor();
		long end = System.currentTimeMillis();
		logger.info("Finished Task. Total: " + (end - begin) + " ms");
	}

	private void processTop10OverMaxSpeedMonitor() {
		SparkConf conf = new SparkConf();
		conf.setMaster(Common.MASTER_NAME).setAppName(Common.APP_NAME_TOP10_OVER_MAX_SPEEDMONITOR);
		JavaSparkContext jsc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(jsc);
		//load data
		loadData(jsc, sqlContext);
		
		DataFrame vehicleLogDataFrame = sqlContext.sql("select monitorId,vehicleSpeed from " + Common.T_VEHICLE_LOG + " where vehicleSpeed > " + Common.MAX_SPEED);
		
		
		
	}
	
	private void loadData(JavaSparkContext jsc, SQLContext sqlContext) {
		//load Vehicle Log data
		DataLoadUtils.dataLoad(jsc, sqlContext, false);
	}
	
}
