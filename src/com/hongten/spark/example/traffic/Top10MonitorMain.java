/**
 * Spark Example  
 * Mail: hongtenzone@foxmail.com 
 * Blog: http://www.cnblogs.com/hongten
 */
package com.hongten.spark.example.traffic;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
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
 * 需求: 在所有监控点里面，通过车辆最多的10个监控点是什么？
 * 
 * 目的： 可以对这些监控点所在的路段采取一些措施来缓解车流量。
 * 
 * @author Hongten
 * @created 20 Jan, 2019
 */
public class Top10MonitorMain implements Serializable {

	private static final long serialVersionUID = 1L;

	static final Logger logger = Logger.getLogger(Top10MonitorMain.class);

	public static void main(String[] args) {
		long begin = System.currentTimeMillis();
		Top10MonitorMain top10MonitorMain = new Top10MonitorMain();
		top10MonitorMain.processTop10Monitor();
		long end = System.currentTimeMillis();
		logger.info("Finished Task. Total: " + (end - begin) + " ms");
	}

	/**
	 * 通过对数据的分析，处理，获取通过最多的10个监控点信息
	 */
	private void processTop10Monitor() {
		SparkConf conf = new SparkConf();
		conf.setMaster(Common.MASTER_NAME).setAppName(Common.APP_NAME_TOP10_MONITOR);

		JavaSparkContext jsc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(jsc);
		// load data
		loadData(jsc, sqlContext);

		// 处理vehicle log data
		JavaPairRDD<Integer, String> vehicleNumAndMonitorSortByKeyDescRDD = processVehicleLogData(sqlContext);

		// 获取top10监控点车辆数据
		List<Tuple2<Integer, String>> top10List = takeTop10Result(vehicleNumAndMonitorSortByKeyDescRDD);

		// 打印结果
		printResult(top10List);

		jsc.stop();
	}

	private JavaPairRDD<Integer, String> processVehicleLogData(SQLContext sqlContext) {
		JavaRDD<Row> vehicleLogRDD = sqlContext.sql("select monitorId,vehiclePlate from " + Common.T_VEHICLE_LOG).javaRDD();

		// 这里是把所有记录转换为<monitor_id, vehicle_plate>格式的RDD
		JavaPairRDD<String, String> monitorIdAndVehicleLogRDD = monitorIdAndVehicleLogRDD(vehicleLogRDD);

		// <monitor_id, Iterable<vehicle_plate>>
		// 即一个监控点对应多个车牌，也即在这个监控点下面经过了多少辆车
		JavaPairRDD<String, Iterable<String>> monitorIdAndVehicleLogGroupByKeyRDD = monitorIdAndVehicleLogRDD.groupByKey();

		// 获取<vehicleNum, monitor_id>
		// 从这里，我们可以看出来，哪些monitor下面经过了多少车辆了，但是，这是一个乱序的RDD
		JavaPairRDD<Integer, String> vehicleNumAndMonitorRDD = vehicleNumAndMonitorRDD(monitorIdAndVehicleLogGroupByKeyRDD);

		// sort by key desc
		JavaPairRDD<Integer, String> vehicleNumAndMonitorSortByKeyDescRDD = vehicleNumAndMonitorSortByKeyDesc(vehicleNumAndMonitorRDD);
		return vehicleNumAndMonitorSortByKeyDescRDD;
	}

	private JavaPairRDD<String, String> monitorIdAndVehicleLogRDD(JavaRDD<Row> vehicleLogRDD) {
		JavaPairRDD<String, String> monitorIdAndVehicleLogRDD = vehicleLogRDD.mapToPair(new PairFunction<Row, String, String>() {
			private static final long serialVersionUID = 1L;

			// 把Row转换为<monito_id, vehicle_plate>格式
			@Override
			public Tuple2<String, String> call(Row row) throws Exception {
				return new Tuple2<String, String>(row.getAs(Common.MONITOR_ID) + Common.EMPTY, row.getAs(Common.VEHICLE_PLATE) + Common.EMPTY);
			}
		});
		return monitorIdAndVehicleLogRDD;
	}

	private JavaPairRDD<Integer, String> vehicleNumAndMonitorRDD(JavaPairRDD<String, Iterable<String>> monitorIdAndVehicleLogGroupByKeyRDD) {
		JavaPairRDD<Integer, String> vehicleNumAndMonitorRDD = monitorIdAndVehicleLogGroupByKeyRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, Integer, String>() {
			private static final long serialVersionUID = 1L;

			// <vehicleNum, monitor_id>
			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Iterable<String>> tuple2) throws Exception {
				Iterator<String> vehiclePlateIte = tuple2._2.iterator();
				// 一个监控点下面在不同时刻，可以经过相同车牌的车辆
				// 所以，这里不需要对车牌去重
				List<String> vehiclePlateList = new ArrayList<String>();
				while (vehiclePlateIte.hasNext()) {
					vehiclePlateList.add(vehiclePlateIte.next());
				}
				Integer vehicleNum = 0;
				if (vehiclePlateList != null && vehiclePlateList.size() > 0) {
					vehicleNum = vehiclePlateList.size();
				}
				return new Tuple2<Integer, String>(vehicleNum, tuple2._1);
			}
		});
		return vehicleNumAndMonitorRDD;
	}

	private JavaPairRDD<Integer, String> vehicleNumAndMonitorSortByKeyDesc(JavaPairRDD<Integer, String> vehicleNumAndMonitorRDD) {
		// sortByKey(boolean ascending) -- 默认是升序排序
		// 现在设置为降序排序
		// 如果我们需要获取到：
		// 通过车辆最少的10个监控点是什么的时候，我们就可以使用vehicleNumAndMonitorRDD.sortByKey();即可
		return vehicleNumAndMonitorRDD.sortByKey(false);
	}

	private List<Tuple2<Integer, String>> takeTop10Result(JavaPairRDD<Integer, String> vehicleNumAndMonitorSortByKeyDescRDD) {
		return vehicleNumAndMonitorSortByKeyDescRDD.take(10);
	}

	/**
	output:
		Top 10 Monitor : 20093, Vehicle Number : 734
		Top 10 Monitor : 20077, Vehicle Number : 690
		Top 10 Monitor : 20009, Vehicle Number : 684
		Top 10 Monitor : 20050, Vehicle Number : 393
		Top 10 Monitor : 20104, Vehicle Number : 390
		Top 10 Monitor : 20079, Vehicle Number : 389
		Top 10 Monitor : 20057, Vehicle Number : 385
		Top 10 Monitor : 20045, Vehicle Number : 381
		Top 10 Monitor : 20044, Vehicle Number : 378
		Top 10 Monitor : 20028, Vehicle Number : 377
	 */
	private void printResult(List<Tuple2<Integer, String>> top10List) {
		if (top10List != null && top10List.size() > 0) {
			for (Tuple2<Integer, String> tuple2 : top10List) {
				logger.info("Top 10 Monitor : " + tuple2._2 + ", Vehicle Number : " + tuple2._1);
			}
		}
	}

	private void loadData(JavaSparkContext jsc, SQLContext sqlContext) {
		// only load Vehicle Log data
		DataLoadUtils.dataLoad(jsc, sqlContext, false);
	}
}
