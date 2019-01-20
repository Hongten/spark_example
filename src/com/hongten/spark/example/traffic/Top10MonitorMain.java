/**
 * Spark Example  
 * Mail: hongtenzone@foxmail.com 
 * Blog: http://www.cnblogs.com/hongten
 */
package com.hongten.spark.example.traffic;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import com.hongten.spark.example.traffic.datagenerate.Common;
import com.hongten.spark.example.traffic.datagenerate.util.DataLoadUtils;

/**
 * 需求: 在所有监控点里面，通过车辆最多的10个监控点是什么？
 * 
 * @author Hongten
 * @created 20 Jan, 2019
 */
public class Top10MonitorMain {

	static final Logger logger = Logger.getLogger(Top10MonitorMain.class);

	public static void main(String[] args) {
		long begin = System.currentTimeMillis();
		processTop10Monitor();
		long end = System.currentTimeMillis();
		logger.info("Finished Task. Total: " + (end - begin) + " ms");
	}

	/**
	 * 通过对数据的分析，处理，获取通过最多的10个监控点信息
	 */
	private static void processTop10Monitor() {
		SparkConf conf = new SparkConf();
		conf.setMaster(Common.MASTER_NAME).setAppName(Common.APP_NAME_TOP10_MONITOR);

		JavaSparkContext jsc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(jsc);
		loadData(jsc, sqlContext);

		jsc.stop();
	}

	private static void loadData(JavaSparkContext jsc, SQLContext sqlContext) {
		DataLoadUtils.dataLoad(jsc, sqlContext);
	}

}
