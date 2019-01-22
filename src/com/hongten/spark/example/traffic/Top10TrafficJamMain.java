/**
 * Spark Example  
 * Mail: hongtenzone@foxmail.com 
 * Blog: http://www.cnblogs.com/hongten
 */
package com.hongten.spark.example.traffic;

import java.io.Serializable;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.hongten.spark.example.traffic.datagenerate.Common;
import com.hongten.spark.example.traffic.datagenerate.util.DataLoadUtils;

/**
 * 需求： 在所有监控点里面，车速行驶缓慢，取前10个交通拥堵的监控点及对应的车辆数。假设车速在 1<=vehicle speed<=15 即为行驶缓慢。<br>
 * 
 * 目的： 对监控点车速进行监控，可以实时获取交通拥堵情况。相关部门可以对交通拥堵情况采取措施。比如通过广播方式，让司机改道。<br>
 * 
 * @author Hongten
 * @created 22 Jan, 2019
 */
public class Top10TrafficJamMain implements Serializable {

	private static final long serialVersionUID = 1L;

	static final Logger logger = Logger.getLogger(Top10TrafficJamMain.class);

	public static void main(String[] args) {
		long begin = System.currentTimeMillis();
		Top10TrafficJamMain top10TrafficJam = new Top10TrafficJamMain();
		top10TrafficJam.processTop10TrafficJam();
		long end = System.currentTimeMillis();
		logger.info("Finished Task. Total: " + (end - begin) + " ms");
	}

	private void processTop10TrafficJam() {
		SparkConf conf = new SparkConf();
		conf.setMaster(Common.MASTER_NAME).setAppName(Common.APP_NAME_TOP10_TRAFFIC_JAM);

		JavaSparkContext jsc = new JavaSparkContext(conf);

		SQLContext sqlContext = new SQLContext(jsc);

		// load data
		loadData(jsc, sqlContext);

		JavaRDD<Row> monitorAndVehicleNumRDD = sqlContext.sql("select monitorId,count(vehiclePlate) vehicleNum from " + Common.T_VEHICLE_LOG + " where vehicleSpeed >=1 and vehicleSpeed <= 15 group by monitorId order by vehicleNum desc limit 10").javaRDD();

		//print result
		printResult(monitorAndVehicleNumRDD);

	}

	/**
	 output:
	 	Top 10 Traffic Jam Monitor : 20117, Vehicle Number : 32
		Top 10 Traffic Jam Monitor : 20073, Vehicle Number : 32
		Top 10 Traffic Jam Monitor : 20097, Vehicle Number : 28
		Top 10 Traffic Jam Monitor : 20069, Vehicle Number : 28
		Top 10 Traffic Jam Monitor : 20115, Vehicle Number : 28
		Top 10 Traffic Jam Monitor : 20106, Vehicle Number : 27
		Top 10 Traffic Jam Monitor : 20110, Vehicle Number : 26
		Top 10 Traffic Jam Monitor : 20060, Vehicle Number : 26
		Top 10 Traffic Jam Monitor : 20025, Vehicle Number : 25
		Top 10 Traffic Jam Monitor : 20026, Vehicle Number : 24
	 */
	private void printResult(JavaRDD<Row> monitorAndVehicleNumRDD) {
		monitorAndVehicleNumRDD.foreach(new VoidFunction<Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Row row) throws Exception {
				logger.info("Top 10 Traffic Jam Monitor : " + row.getAs(Common.MONITOR_ID) + ", Vehicle Number : " + row.getAs(Common.VEHICLE_NUM));
			}
		});
	}

	/**
	 * load data
	 */
	private void loadData(JavaSparkContext jsc, SQLContext sqlContext) {
		DataLoadUtils.dataLoad(jsc, sqlContext, false);
	}
}
