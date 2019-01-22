/**
 * Spark Example  
 * Mail: hongtenzone@foxmail.com 
 * Blog: http://www.cnblogs.com/hongten
 */
package com.hongten.spark.example.traffic;

import java.io.Serializable;
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
 * 需求： 在所有监控点里面，在24小时内，查询出每个小时里面车流量，取前10个车流量最大的时段<br>
 * e.g.<br>
 * 		08 -- 319<br>
 *      18 -- 300<br>
 *      12 -- 280<br>
 *      .....<br>
 * 
 * 目的： 了解各个小时内的车流量情况，若果出现车流高峰，应该采取什么样的措施。
 * @author Hongten
 * @created 21 Jan, 2019
 */
public class Top10VehicleNumberHourMain implements Serializable{

	private static final long serialVersionUID = 1L;

	static final Logger logger = Logger.getLogger(Top10VehicleNumberHourMain.class);

	public static void main(String[] args) {
		long begin = System.currentTimeMillis();
		Top10VehicleNumberHourMain top10VehicleNumberHourMain = new Top10VehicleNumberHourMain();
		top10VehicleNumberHourMain.processTop10VehicleNumberHour();
		long end = System.currentTimeMillis();
		logger.info("Finished Task. Total: " + (end - begin) + " ms");
	}

	private void processTop10VehicleNumberHour() {
		SparkConf conf = new SparkConf();
		conf.setMaster(Common.MASTER_NAME).setAppName(Common.APP_NAME_TOP10_VEHICLE_NUMBER_HOUR);

		JavaSparkContext jsc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(jsc);

		// load data
		loadData(jsc, sqlContext);

		JavaPairRDD<Integer, String> vehicleNumAndHourSortByKeyRDD = processVehicleLogData(sqlContext);
		
		//print result
		printResult(vehicleNumAndHourSortByKeyRDD);

		jsc.stop();
	}

	private JavaPairRDD<Integer, String> processVehicleLogData(SQLContext sqlContext) {
		JavaRDD<Row> dateTimeAndVehiclePlateRDD = sqlContext.sql("select dateTime, vehiclePlate from " + Common.T_VEHICLE_LOG).javaRDD();

		// <hour, vehicle_plate>
		// 把查询出来的记录转换为<hour, vehicle_plate>形式
		JavaPairRDD<String, String> hourVehiclePlateRDD = hourVehiclePlateRDD(dateTimeAndVehiclePlateRDD);

		// <hour, Iterable<vehicle_plate>>
		// 对<hour, vehicle_plate>形式进行groupByKey以后，就会获取到在对应的hour下面的vehicle_plate的集合
		JavaPairRDD<String, Iterable<String>> hourVehiclePlateGroupByKeyRDD = hourVehiclePlateRDD.groupByKey();

		// <vehicleNum, hour>
		// 计算出每个hour下面的集合的vehicle个数，返回<vehicleNum, hour>形式，为了对结果进行sortByKey(false)
		JavaPairRDD<Integer, String> vehicleNumAndHourRDD = vehicleNumAndHourRDD(hourVehiclePlateGroupByKeyRDD);
		
		//对<vehicleNum, hour>形式进行排序
		JavaPairRDD<Integer, String> vehicleNumAndHourSortByKeyRDD = vehicleNumAndHourRDD.sortByKey(false);
		return vehicleNumAndHourSortByKeyRDD;
	}

	private JavaPairRDD<String, String> hourVehiclePlateRDD(JavaRDD<Row> dateTimeAndVehiclePlateRDD) {
		JavaPairRDD<String, String> hourVehiclePlateRDD = dateTimeAndVehiclePlateRDD.mapToPair(new PairFunction<Row, String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(Row row) throws Exception {
				// 16/01/2019 10:20:30
				String dateTime = row.getAs(Common.DATE_TIME);
				String hourStr = "";
				if (dateTime != null && dateTime.contains(Common.BLANK)) {
					// [16/01/2019,10:20:30]
					String[] split = dateTime.split(Common.BLANK);
					if (split != null && split.length == 2) {
						// 10:20:30
						String time = split[1];
						if (time != null && time.contains(Common.COLON)) {
							// [10,20,30]
							String[] timeStrs = time.split(Common.COLON);
							if (timeStrs != null && timeStrs.length == 3) {
								// 10
								hourStr = timeStrs[0];
							}
						}
					}
				}
				return new Tuple2<String, String>(hourStr, row.getAs(Common.VEHICLE_PLATE) + Common.EMPTY);
			}
		});
		return hourVehiclePlateRDD;
	}

	private JavaPairRDD<Integer, String> vehicleNumAndHourRDD(JavaPairRDD<String, Iterable<String>> hourVehiclePlateGroupByKeyRDD) {
		JavaPairRDD<Integer, String> vehicleNumAndHourRDD = hourVehiclePlateGroupByKeyRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, Integer, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Iterable<String>> tuple2) throws Exception {
				Iterator<String> vehiclePlateIte = tuple2._2.iterator();
				// 计算出每个hour下面的集合的vehicle个数
				Integer vehicleNum = 0;
				while (vehiclePlateIte.hasNext()) {
					vehiclePlateIte.next();
					vehicleNum++;
				}
				// <vehicleNum, hour>
				// 返回<vehicleNum, hour>形式，为了对结果进行sortByKey(false)
				return new Tuple2<Integer, String>(vehicleNum, tuple2._1);
			}
		});
		return vehicleNumAndHourRDD;
	}

	/**
	 output:
		Top 10 Hour : 03, Vehicle Number : 1797
		Top 10 Hour : 13, Vehicle Number : 1786
		Top 10 Hour : 07, Vehicle Number : 1781
		Top 10 Hour : 09, Vehicle Number : 1776
		Top 10 Hour : 00, Vehicle Number : 1775
		Top 10 Hour : 04, Vehicle Number : 1767
		Top 10 Hour : 14, Vehicle Number : 1766
		Top 10 Hour : 11, Vehicle Number : 1763
		Top 10 Hour : 02, Vehicle Number : 1762
		Top 10 Hour : 17, Vehicle Number : 1762
	 */
	private void printResult(JavaPairRDD<Integer, String> vehicleNumAndHourSortByKeyRDD) {
		List<Tuple2<Integer, String>> top10Result = vehicleNumAndHourSortByKeyRDD.take(10);

		if (top10Result != null && top10Result.size() > 0) {
			for (Tuple2<Integer, String> tuple2 : top10Result) {
				logger.info("Top 10 Hour : " + tuple2._2 + ", Vehicle Number : " + tuple2._1);
			}
		}
	}
	
	private void loadData(JavaSparkContext jsc, SQLContext sqlContext) {
		// load Vehicle Log data
		DataLoadUtils.dataLoad(jsc, sqlContext, false);
	}
	
}
