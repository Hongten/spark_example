/**
 * Spark Example  
 * Mail: hongtenzone@foxmail.com 
 * Blog: http://www.cnblogs.com/hongten
 */
package com.hongten.spark.example.traffic;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

import com.hongten.spark.example.traffic.datagenerate.Common;
import com.hongten.spark.example.traffic.datagenerate.util.DataLoadUtils;
import com.hongten.spark.example.traffic.datagenerate.vo.TrafficJamVO;

/**
 * 需求： 在所有监控点里面，随机抽取10辆车，然后计算出这些车的车辆轨迹。<br>
 * 
 * 目的： 对公交车轨迹分析，可以知道该公交车是否有改道行为。对可疑车辆进行轨迹追踪，协助有关部门破案。<br>
 * 
 * @author Hongten
 * @created 22 Jan, 2019
 */
public class Random10VehicleTracingMain implements Serializable {

	private static final long serialVersionUID = 1L;

	static final Logger logger = Logger.getLogger(Random10VehicleTracingMain.class);

	public static void main(String[] args) {
		long begin = System.currentTimeMillis();
		Random10VehicleTracingMain random10VehicleTracing = new Random10VehicleTracingMain();
		random10VehicleTracing.processRandom10VehicleTracing();
		long end = System.currentTimeMillis();
		logger.info("Finished Task. Total: " + (end - begin) + " ms");
	}

	private void processRandom10VehicleTracing() {
		SparkConf conf = new SparkConf();
		conf.setMaster(Common.MASTER_NAME).setAppName(Common.APP_NAME_RANDOM_10_VEHICLE_TRACING);

		JavaSparkContext jsc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(jsc);

		// load data
		loadData(jsc, sqlContext);

		// 获取所有的车牌
		JavaRDD<Row> vehiclePlateRDD = sqlContext.sql("select distinct vehiclePlate from " + Common.T_VEHICLE_LOG).javaRDD();

		// 拿去出所有的车牌
		List<Row> vehiclePlateTakeList = vehiclePlateRDD.take(Common.VEHICLE_NUMBER);

		// 随机选取出车牌号码
		List<String> randomVehiclePlateList = getRandom10VehiclePlateList(vehiclePlateTakeList);

		// 把车牌从Driver端广播出去到Executor端
		final Broadcast<List<String>> roadFlowBroadcast = jsc.broadcast(randomVehiclePlateList);

		JavaRDD<Row> vehicleLogDataRDD = sqlContext.sql("select vehiclePlate,dateTime,monitorId from " + Common.T_VEHICLE_LOG).javaRDD();

		// Executor端获取到广播变量里面的10个随机车牌
		// 对vehicleLogDataRDD进行过滤，过滤出10个随机车牌的所有记录
		JavaRDD<Row> vehicleLogDataFilterRDD = vehicleLogDataFilter(roadFlowBroadcast, vehicleLogDataRDD);

		// 获取到<vehicle_plate, Row>的形式
		// 以便对vehicle_plate进行group by key计算
		JavaPairRDD<String, Row> vehiclePlateAndRowRDD = vehiclePlateAndRow(vehicleLogDataFilterRDD);

		// <vehicle_plate, Iterable<Row>>形式
		// 现在可以知道，每一个车牌的下对应的所有数据，不过这些数据都是无序的
		// 所以接下来，我们要对这些数据进行排序操作
		JavaPairRDD<String, Iterable<Row>> vehiclePlateAndRowGroupByKeyRDD = vehiclePlateAndRowRDD.groupByKey();

		// 返回<vehile_plate, "monitor_id1,monitor_id2">形式
		JavaPairRDD<String, String> resultRDD = getRandom10VehicleTracingResult(vehiclePlateAndRowGroupByKeyRDD);

		// print result
		printResult(resultRDD);

		jsc.stop();

	}

	private List<String> getRandom10VehiclePlateList(List<Row> vehiclePlateTakeList) {
		List<String> randomVehiclePlateList = new ArrayList<String>();
		Random random = new Random();
		if (vehiclePlateTakeList != null && vehiclePlateTakeList.size() > 0) {
			// 循环10次，获取10个随机车牌
			for (int i = 0; i < 10; i++) {
				getRandomVehiclePlate(vehiclePlateTakeList, randomVehiclePlateList, random);
			}
		}
		return randomVehiclePlateList;
	}

	private void getRandomVehiclePlate(List<Row> vehiclePlateTakeList, List<String> randomVehiclePlateList, Random random) {
		String vehiclePlate = vehiclePlateTakeList.get(random.nextInt(vehiclePlateTakeList.size())).getAs(Common.VEHICLE_PLATE);
		if (randomVehiclePlateList.contains(vehiclePlate)) {
			// 避免添加重复的车牌
			getRandomVehiclePlate(vehiclePlateTakeList, randomVehiclePlateList, random);
		} else {
			randomVehiclePlateList.add(vehiclePlate);
		}
	}

	private JavaRDD<Row> vehicleLogDataFilter(final Broadcast<List<String>> roadFlowBroadcast, JavaRDD<Row> vehicleLogDataRDD) {
		JavaRDD<Row> vehicleLogDataFilterRDD = vehicleLogDataRDD.filter(new Function<Row, Boolean>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Row row) throws Exception {
				// Executor端使用广播变量
				return roadFlowBroadcast.value().contains(row.getAs(Common.VEHICLE_PLATE));
			}
		});
		return vehicleLogDataFilterRDD;
	}

	private JavaPairRDD<String, Row> vehiclePlateAndRow(JavaRDD<Row> vehicleLogDataFilterRDD) {
		JavaPairRDD<String, Row> vehiclePlateAndRowRDD = vehicleLogDataFilterRDD.mapToPair(new PairFunction<Row, String, Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Row> call(Row row) throws Exception {
				// <vehicle_plate, Row>
				return new Tuple2<String, Row>(row.getAs(Common.VEHICLE_PLATE) + Common.EMPTY, row);
			}
		});
		return vehiclePlateAndRowRDD;
	}

	private JavaPairRDD<String, String> getRandom10VehicleTracingResult(JavaPairRDD<String, Iterable<Row>> vehiclePlateAndRowGroupByKeyRDD) {
		JavaPairRDD<String, String> resultRDD = vehiclePlateAndRowGroupByKeyRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(Tuple2<String, Iterable<Row>> tuple2) throws Exception {
				Iterator<Row> rowIte = tuple2._2.iterator();
				List<TrafficJamVO> trafficJamVOList = new ArrayList<TrafficJamVO>();
				while (rowIte.hasNext()) {
					// SKJ28792C,19/01/2019 15:53:35,20001
					Row row = rowIte.next();
					// 19/01/2019 15:53:35
					String dateTimeStr = row.getAs(Common.DATE_TIME);
					// 219155335
					String dateTime = dateTimeStr.substring(6, 7) + dateTimeStr.substring(0, 2) + dateTimeStr.substring(11, 13) + dateTimeStr.substring(14, 16) + dateTimeStr.substring(17, 19);
					// 20001
					String monitorId = row.getAs(Common.MONITOR_ID);
					// 降序排序
					TrafficJamVO trafficJamVO = new TrafficJamVO(dateTimeStr, Integer.valueOf(dateTime), Integer.valueOf(monitorId), false);
					trafficJamVOList.add(trafficJamVO);
				}

				StringBuffer monitorIdSb = new StringBuffer();
				String monitorIdStr = Common.EMPTY;
				if (trafficJamVOList != null && trafficJamVOList.size() > 0) {
					// 对vo进行排序
					Collections.sort(trafficJamVOList);

					for (TrafficJamVO vo : trafficJamVOList) {
						// 19/01/2019 15:53:35 @ 20001 ->
						monitorIdSb.append(vo.getDateTimeStr()).append(Common.BLANK).append(Common.AT).append(Common.BLANK).append(vo.getMonitorId()).append(Common.MINUS).append(Common.GREATER_THAN_SIGN);
					}

					monitorIdStr = monitorIdSb.toString();
					// end with '->'
					monitorIdStr = monitorIdStr.substring(0, monitorIdStr.length() - 2);
				}
				// <vehile_plate, "monitor_id1,monitor_id2">
				return new Tuple2<String, String>(tuple2._1, monitorIdStr);
			}
		});
		return resultRDD;
	}

	/**
	output:
		Random 10 Vehicle Plate : SPC04739U, Vehicle Tracing : 22/01/2019 06:57:55 @ 20083->22/01/2019 10:39:09 @ 20045->22/01/2019 11:31:54 @ 20104->22/01/2019 11:50:28 @ 20018->22/01/2019 13:12:50 @ 20092->22/01/2019 14:53:43 @ 20090->22/01/2019 20:24:35 @ 20076->22/01/2019 23:15:01 @ 20105->22/01/2019 23:20:10 @ 20054
		Random 10 Vehicle Plate : SYF41396E, Vehicle Tracing : 22/01/2019 12:08:14 @ 20009->22/01/2019 20:12:03 @ 20110->22/01/2019 22:15:23 @ 20081
		Random 10 Vehicle Plate : STF20640M, Vehicle Tracing : 22/01/2019 04:39:01 @ 20005->22/01/2019 04:43:23 @ 20043->22/01/2019 06:06:00 @ 20101->22/01/2019 14:46:07 @ 20093->22/01/2019 17:05:28 @ 20064->22/01/2019 18:35:17 @ 20116->22/01/2019 20:31:57 @ 20084->22/01/2019 22:53:21 @ 20074
		Random 10 Vehicle Plate : SLH31785H, Vehicle Tracing : 22/01/2019 02:45:25 @ 20017->22/01/2019 04:15:37 @ 20091->22/01/2019 07:25:41 @ 20004->22/01/2019 07:43:51 @ 20001->22/01/2019 10:45:10 @ 20082->22/01/2019 21:38:15 @ 20046->22/01/2019 21:40:49 @ 20108->22/01/2019 23:49:53 @ 20061
		Random 10 Vehicle Plate : SLK86461C, Vehicle Tracing : 22/01/2019 03:40:05 @ 20021->22/01/2019 04:52:07 @ 20105->22/01/2019 07:40:08 @ 20007->22/01/2019 20:21:43 @ 20056->22/01/2019 20:29:15 @ 20082->22/01/2019 20:58:43 @ 20108
		Random 10 Vehicle Plate : SCY31301M, Vehicle Tracing : 22/01/2019 00:30:42 @ 20093->22/01/2019 00:31:20 @ 20110->22/01/2019 01:11:58 @ 20097->22/01/2019 01:44:17 @ 20023->22/01/2019 02:34:48 @ 20072->22/01/2019 04:15:33 @ 20115->22/01/2019 05:56:28 @ 20112->22/01/2019 07:15:40 @ 20079->22/01/2019 09:33:14 @ 20091->22/01/2019 11:22:41 @ 20097->22/01/2019 11:44:15 @ 20060->22/01/2019 15:59:52 @ 20111->22/01/2019 18:06:04 @ 20042->22/01/2019 18:27:12 @ 20030->22/01/2019 19:40:52 @ 20040->22/01/2019 21:44:30 @ 20017->22/01/2019 23:25:26 @ 20013
		Random 10 Vehicle Plate : SPP49360K, Vehicle Tracing : 22/01/2019 03:36:56 @ 20114->22/01/2019 06:17:43 @ 20091->22/01/2019 13:58:15 @ 20089->22/01/2019 16:03:37 @ 20080->22/01/2019 16:44:18 @ 20042->22/01/2019 17:03:43 @ 20062->22/01/2019 21:40:12 @ 20096
		Random 10 Vehicle Plate : SPZ50729A, Vehicle Tracing : 22/01/2019 00:57:45 @ 20020->22/01/2019 05:58:15 @ 20011->22/01/2019 09:55:39 @ 20064->22/01/2019 12:14:12 @ 20077->22/01/2019 16:13:26 @ 20024->22/01/2019 16:15:39 @ 20013->22/01/2019 18:49:22 @ 20107->22/01/2019 20:20:36 @ 20009->22/01/2019 22:05:53 @ 20045
		Random 10 Vehicle Plate : STC02291T, Vehicle Tracing : 22/01/2019 01:29:53 @ 20076->22/01/2019 05:56:21 @ 20053->22/01/2019 09:39:04 @ 20039->22/01/2019 10:52:45 @ 20065->22/01/2019 13:31:06 @ 20118->22/01/2019 20:06:13 @ 20114->22/01/2019 20:40:57 @ 20083->22/01/2019 23:14:11 @ 20063
		Random 10 Vehicle Plate : SXE75436A, Vehicle Tracing : 22/01/2019 00:32:37 @ 20089->22/01/2019 01:34:43 @ 20091->22/01/2019 04:32:19 @ 20087->22/01/2019 04:36:21 @ 20004->22/01/2019 06:27:19 @ 20120->22/01/2019 13:26:34 @ 20096->22/01/2019 15:46:38 @ 20051->22/01/2019 16:43:26 @ 20051
	 */
	private void printResult(JavaPairRDD<String, String> resultRDD) {
		resultRDD.foreach(new VoidFunction<Tuple2<String, String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, String> tuple2) throws Exception {
				logger.info("Random 10 Vehicle Plate : " + tuple2._1 + ", Vehicle Tracing : " + tuple2._2);
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
