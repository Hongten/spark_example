/**
 * Spark Example  
 * Mail: hongtenzone@foxmail.com 
 * Blog: http://www.cnblogs.com/hongten
 */
package com.hongten.spark.example.traffic;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
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
 * 需求： 在所有监控点里面，这些监控点直接的转化率是多少？<br>
 * 例如：给出监控点A和B，要知道监控点A到B的转化率，那么我们要知道通过监控点A后随即通过监控点B的车辆数，<br>
 * 还需要知道通过监控点A的所有车辆数(因为通过监控点A的车，接下来可能不通过监控点B，而是其他监控点)。<br>
 * 
 * 目的： 检测给出的两个监控点的设计是否合理。如果转化率太低，说明这样的设计不是很合理，<br>
 * 那么可以通过采取一些措施，使得资源得到充分利用。<br>
 * 
 * 我们可以查看{@link Random10VehicleTracingMain}的其中一条运行结果:<br>
 * Random 10 Vehicle Plate : SYF41396E, Vehicle Tracing : 22/01/2019 12:08:14 @ 20009->22/01/2019 20:12:03 @ 20110->22/01/2019 22:15:23 @ 20081<br>
 * 可以知道车牌SYF41396E通过20009监控点(A)后，接下来去到了20110监控点(B)。<br>
 * 那么我们就来计算监控点A(20009)到监控点B(20110)的转化率。<br>
 * 
 * 
 * @see com.hongten.spark.example.traffic.Random10VehicleTracingMain
 * 
 * @author Hongten
 * @created 22 Jan, 2019
 */
public class MonitorConversionRateMain implements Serializable {

	private static final long serialVersionUID = 1L;

	//这里可以自定义
	String monitorIdA = "20009";
	String monitorIdB = "20110";

	static final Logger logger = Logger.getLogger(Random10VehicleTracingMain.class);

	public static void main(String[] args) {
		long begin = System.currentTimeMillis();
		MonitorConversionRateMain monitorConversionRate = new MonitorConversionRateMain();
		monitorConversionRate.processMonitorConversionRate();
		long end = System.currentTimeMillis();
		logger.info("Finished Task. Total: " + (end - begin) + " ms");
	}

	private void processMonitorConversionRate() {
		SparkConf conf = new SparkConf();
		conf.setMaster(Common.MASTER_NAME).setAppName(Common.APP_NAME_RANDOM_10_VEHICLE_TRACING);

		JavaSparkContext jsc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(jsc);

		// load data
		loadData(jsc, sqlContext);

		// 把车牌从Driver端广播出去到Executor端
		// "A"格式
		final Broadcast<String> monitorIdABroadcast = jsc.broadcast(monitorIdA);
		// "A->B"格式
		final Broadcast<String> monitorIdBBroadcast = jsc.broadcast(monitorIdA + Common.MINUS + Common.GREATER_THAN_SIGN + monitorIdB);

		JavaRDD<Row> vehicleLogDataRDD = sqlContext.sql("select vehiclePlate,dateTime,monitorId from " + Common.T_VEHICLE_LOG).javaRDD();

		// <vehicle_plate, Row>格式
		JavaPairRDD<String, Row> vehiclePlateRowRDD = vehiclePlateRow(vehicleLogDataRDD);

		// <vehicle_plate, Iterable<Row>>
		JavaPairRDD<String, Iterable<Row>> vehiclePlateRowGroupByKeyRDD = vehiclePlateRowRDD.groupByKey();

		JavaPairRDD<Integer, Integer> resultRdd = getResultRdd(monitorIdABroadcast, monitorIdBBroadcast, vehiclePlateRowGroupByKeyRDD);

		printResult(monitorIdBBroadcast, resultRdd);

		jsc.stop();
	}

	private JavaPairRDD<String, Row> vehiclePlateRow(JavaRDD<Row> vehicleLogDataRDD) {
		JavaPairRDD<String, Row> vehiclePlateRowRDD = vehicleLogDataRDD.mapToPair(new PairFunction<Row, String, Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Row> call(Row row) throws Exception {
				//<vehicle_plate, Row>
				return new Tuple2<String, Row>(row.getAs(Common.VEHICLE_PLATE)+Common.EMPTY, row);
			}
		});
		return vehiclePlateRowRDD;
	}

	/**
	 * 统计监控点A的车辆数
	 * 统计监控点A到B的车辆数
	 */
	private JavaPairRDD<Integer, Integer> getResultRdd(final Broadcast<String> monitorIdABroadcast, final Broadcast<String> monitorIdBBroadcast, JavaPairRDD<String, Iterable<Row>> vehiclePlateRowGroupByKeyRDD) {
		JavaPairRDD<Integer, Integer> resultRdd = vehiclePlateRowGroupByKeyRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, Iterable<Row>>>, Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<Integer, Integer>> call(Iterator<Tuple2<String, Iterable<Row>>> ite) throws Exception {
				Integer monitorATotal = 0;
				Integer monitorAToBTotal = 0;

				while (ite.hasNext()) {
					Tuple2<String, Iterable<Row>> tuple2 = ite.next();
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
							// 只要进过监控点A，那么monitorATotal增加1
							// 对monitorA经过车辆进行统计
							if (monitorIdABroadcast.value().equals(vo.getMonitorId() + Common.EMPTY)) {
								monitorATotal++;
							}
							// 20001->20002
							monitorIdSb.append(vo.getMonitorId()).append(Common.MINUS).append(Common.GREATER_THAN_SIGN);
						}

						monitorIdStr = monitorIdSb.toString();
						// end with '->'
						monitorIdStr = monitorIdStr.substring(0, monitorIdStr.length() - 2);
						// 统计"A->B"的数量
						if (monitorIdStr.contains(monitorIdBBroadcast.value())) {
							String[] split = monitorIdStr.split(monitorIdBBroadcast.value());
							if (split != null && split.length > 0) {
								// "A->B->A->B->C".split("A->B").length = 3
								monitorAToBTotal += (split.length - 1);
							}
						}
					}
				}
				return Arrays.asList(new Tuple2<Integer, Integer>(monitorATotal, monitorAToBTotal));
			}
		});
		return resultRdd;
	}

	/**
	 * 通过结果我们可以看出，这两个监控点的转化率非常低，设计的很不合理呀！！！
	 output:
	 	Monitor (A->B): 20009->20110, MonitorATotal : 684, MonitorAToBTotal : 5, Conversion Rate : 0.73099%
	 */
	private void printResult(final Broadcast<String> monitorIdBBroadcast, JavaPairRDD<Integer, Integer> resultRdd) {
		resultRdd.foreach(new VoidFunction<Tuple2<Integer,Integer>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<Integer, Integer> tuple2) throws Exception {
				DecimalFormat df = new DecimalFormat(Common.DECIMAL_FORMAT);
				logger.info("Monitor (A->B): " + monitorIdBBroadcast.value() + ", MonitorATotal : " + tuple2._1() + ", MonitorAToBTotal : " + tuple2._2() + ", Conversion Rate : " + df.format(((double)tuple2._2()/tuple2._1())*100) + "%");
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
