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
 * 需求： 在所有监控点里面，超速（max speed: 250）车辆最多的10个监控点是什么？
 * 
 * 目的： 知道了结果以后，相关人员可以对这些监控点所在的路段进行分析，并采取相关措施来限制车辆超速。比如：加设减速带等
 * 
 * @author Hongten
 * @created 21 Jan, 2019
 */
public class Top10OverSpeedVehicleNumberMain implements Serializable {

	private static final long serialVersionUID = 1L;

	static final Logger logger = Logger.getLogger(Top10OverSpeedVehicleNumberMain.class);

	public static void main(String[] args) {
		long begin = System.currentTimeMillis();
		Top10OverSpeedVehicleNumberMain top10OverSpeedVehicleNumberMain = new Top10OverSpeedVehicleNumberMain();
		top10OverSpeedVehicleNumberMain.processTop10OverSpeedVehicleNumber();
		long end = System.currentTimeMillis();
		logger.info("Finished Task. Total: " + (end - begin) + " ms");
	}

	private void processTop10OverSpeedVehicleNumber() {
		SparkConf conf = new SparkConf();
		conf.setMaster(Common.MASTER_NAME).setAppName(Common.APP_NAME_TOP10_OVER_SPEED_VEHICLE_NUMBER);

		JavaSparkContext jsc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(jsc);

		// load data
		loadData(jsc, sqlContext);
		// query top 10 Over speed vehicle number
		// 这里利用sql帮助我们查询出所需要的结果
		// 如果我们不使用sql，可以参考 - com.hongten.spark.example.traffic.Top10OverSpeedMonitorMain
		// 这时，我们就需要去调用一些算子来帮助我们实现 - groupByKey(), sortByKey(), mapToPair()等待
		JavaRDD<Row> monitorAndVehicleNumberRDD = sqlContext.sql("select monitorId,count(vehicleSpeed) vehicleNum from " + Common.T_VEHICLE_LOG + " where vehicleSpeed > " + Common.OVER_SPEED + " group by monitorId order by vehicleNum desc limit 10").javaRDD();

		// print result
		printResult(monitorAndVehicleNumberRDD);

	}

	/**
	output:
		Top 10 Over Speed Monitor : 20073 , Vehicle Number : 123
		Top 10 Over Speed Monitor : 20117 , Vehicle Number : 118
		Top 10 Over Speed Monitor : 20097 , Vehicle Number : 117
		Top 10 Over Speed Monitor : 20114 , Vehicle Number : 76
		Top 10 Over Speed Monitor : 20005 , Vehicle Number : 75
		Top 10 Over Speed Monitor : 20106 , Vehicle Number : 72
		Top 10 Over Speed Monitor : 20009 , Vehicle Number : 72
		Top 10 Over Speed Monitor : 20091 , Vehicle Number : 72
		Top 10 Over Speed Monitor : 20062 , Vehicle Number : 71
		Top 10 Over Speed Monitor : 20008 , Vehicle Number : 71
	 */
	private void printResult(JavaRDD<Row> monitorAndVehicleNumberRDD) {
		monitorAndVehicleNumberRDD.foreach(new VoidFunction<Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Row row) throws Exception {
				logger.info("Top 10 Over Speed Monitor : " + row.getAs(Common.MONITOR_ID) + " , Vehicle Number : " + row.getAs(Common.VEHICLE_NUM));
			}
		});
	}

	private void loadData(JavaSparkContext jsc, SQLContext sqlContext) {
		// load Vehicle Log data
		DataLoadUtils.dataLoad(jsc, sqlContext, false);
	}
}
