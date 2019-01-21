/**
 * Spark Example  
 * Mail: hongtenzone@foxmail.com 
 * Blog: http://www.cnblogs.com/hongten
 */
package com.hongten.spark.example.traffic;

import java.io.Serializable;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

import com.hongten.spark.example.traffic.datagenerate.Common;
import com.hongten.spark.example.traffic.datagenerate.util.DataLoadUtils;

/**
 * 需求: 在所有监控点里面，超速（max speed: 250）车辆中车速最大的10个监控点是什么？
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
		// load data
		loadData(jsc, sqlContext);
		
		//process vehicle log data
		JavaPairRDD<Integer, String> vehicleSpeedAndMonitorRDD = processVehicleLogRDD(sqlContext);

		//sort by key(vehicle speed desc)
		JavaPairRDD<Integer, String> vehicleSpeedAndMonitorSortByKeyRDD = vehicleSpeedAndMonitorRDD.sortByKey(false);
		
		//get top 10 result
		List<Tuple2<Integer, String>> top10OverMaxSpeedMonitor = vehicleSpeedAndMonitorSortByKeyRDD.take(10);
		
		//print result
		printTop10OverMaxSpeedMonitorResult(top10OverMaxSpeedMonitor);
	}

	private JavaPairRDD<Integer, String> processVehicleLogRDD(SQLContext sqlContext) {
		//query from t_vehicle_log
		JavaRDD<Row> vehicleLogRDD = sqlContext.sql("select monitorId,vehicleSpeed from " + Common.T_VEHICLE_LOG + " where vehicleSpeed > " + Common.MAX_SPEED).javaRDD();
		return vehicleSppedAndMonitorRDD(vehicleLogRDD);
	}

	/**
	 * <vehicle_speed, monitor_id>形式
	 * 这样可以使用sortBykey，对vehicle_speed进行排序
	 */
	private JavaPairRDD<Integer, String> vehicleSppedAndMonitorRDD(JavaRDD<Row> vehicleLogRDD) {
		JavaPairRDD<Integer, String> vehicleSpeedAndMonitorRDD = vehicleLogRDD.mapToPair(new PairFunction<Row, Integer, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(Row row) throws Exception {
				return new Tuple2<Integer, String>((Integer) row.getAs(Common.VEHICLE_SPEED), row.getAs(Common.MONITOR_ID) + Common.EMPTY);
			}
		});
		return vehicleSpeedAndMonitorRDD;
	}

	/**
	output:
		Top 10 Over Max Speed Monitor : 20031, Vehicle Speed : 300
		Top 10 Over Max Speed Monitor : 20015, Vehicle Speed : 300
		Top 10 Over Max Speed Monitor : 20063, Vehicle Speed : 300
		Top 10 Over Max Speed Monitor : 20120, Vehicle Speed : 300
		Top 10 Over Max Speed Monitor : 20049, Vehicle Speed : 300
		Top 10 Over Max Speed Monitor : 20073, Vehicle Speed : 300
		Top 10 Over Max Speed Monitor : 20009, Vehicle Speed : 300
		Top 10 Over Max Speed Monitor : 20032, Vehicle Speed : 300
		Top 10 Over Max Speed Monitor : 20106, Vehicle Speed : 300
		Top 10 Over Max Speed Monitor : 20036, Vehicle Speed : 300
	 */
	private void printTop10OverMaxSpeedMonitorResult(List<Tuple2<Integer, String>> top10OverMaxSpeedMonitor) {
		if (top10OverMaxSpeedMonitor != null && top10OverMaxSpeedMonitor.size() > 0) {
			for (Tuple2<Integer, String> tuple2 : top10OverMaxSpeedMonitor) {
				logger.info("Top 10 Over Max Speed Monitor : " + tuple2._2 + ", Vehicle Speed : " + tuple2._1());
			}
		}
	}
	
	private void loadData(JavaSparkContext jsc, SQLContext sqlContext) {
		//load Vehicle Log data
		DataLoadUtils.dataLoad(jsc, sqlContext, false);
	}
	
}
