/**
 * Spark Example  
 * Mail: hongtenzone@foxmail.com 
 * Blog: http://www.cnblogs.com/hongten
 */
package com.hongten.spark.example.streaming;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

import com.hongten.spark.example.streaming.util.RealTimeDataGenerateUtils;
import com.hongten.spark.example.traffic.datagenerate.Common;

/**
 * 需求：使用SparkStreaming，并且结合Kafka，获取实时道路交通拥堵情况信息。<br><br>
 * 
 * 目的： 对监控点平均车速进行监控，可以实时获取交通拥堵情况信息。相关部门可以对交通拥堵情况采取措施。<br>
 * e.g.1.通过广播方式，让司机改道。<br>
 * 2.通过实时交通拥堵情况数据，反映在一些APP上面，形成实时交通拥堵情况地图，方便用户查询。<br><br>
 * 
 * 使用说明：<br>
 * 
 * Step 1: 在node1,node2,node2中启动Zookeeper<br>
 * .zkServer.sh start                      <br><br>
 * 
 * 在kafka中的相关操作<br>
 * Step 2: 创建topic： spark-real-time-vehicle-log<br>
 * cd /home/kafka-2.10/bin                     <br>
 * ./kafka-topics.sh --zookeeper node1,node2,node3 --create --topic spark-real-time-vehicle-log --partitions 3 --replication-factor 3
 * <br>
 * Step 3: 查看所有topic<br>
 * ./kafka-topics.sh --zookeeper node1,node2,node3 --list
 * <br>
 * Step 4: 监测topic： spark-real-time-vehicle-log<br>
 * ./kafka-console-consumer.sh --zookeeper node1,node2,node3 --from-beginning --topic spark-real-time-vehicle-log
 * <br>
 * 
 * Step 5： 运行RealTimeVehicleSpeedMonitorMain<br>
 * 
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

		SparkConf conf = new SparkConf();
		conf.setMaster(Common.MASTER_NAME).setAppName(Common.APP_NAME_REAL_TIME_TRAFFIC_JAM_STATUS);
		conf.set("spark.local.dir", Common.SPARK_CHECK_POINT_DIR);

		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(Common.SPARK_STREAMING_PROCESS_DATA_FREQUENCY));
		// 日志级别
		jsc.sparkContext().setLogLevel(Common.SPARK_STREAMING_LOG_LEVEL);
		// checkpoint
		jsc.checkpoint(Common.SPARK_CHECK_POINT_DIR);

		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put(Common.METADATA_BROKER_LIST, Common.METADATA_BROKER_LIST_VALUE);

		Set<String> topicSet = new HashSet<String>();
		topicSet.add(Common.KAFKA_TOPIC_SPARK_REAL_TIME_VEHICLE_LOG);

		JavaPairInputDStream<String, String> carRealTimeLogDStream = KafkaUtils.createDirectStream(jsc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicSet);

		// 获取行： 2019-01-22 18:15:18 SSX14139U 141 10006 20011 4002
		JavaDStream<String> vehicleLogDStream = vehicleLogDstream(carRealTimeLogDStream);

		// 移除脏数据
		JavaDStream<String> vehicleLogFilterDStream = filter(vehicleLogDStream);

		// 返回<monitor_id, vehicle_speed>格式
		JavaPairDStream<String, Integer> monitorAndVehicleSpeedDStream = monitorAndVehicleSpeedDstream(vehicleLogFilterDStream);

		// 对key进行处理
		JavaPairDStream<String, Tuple2<Integer, Integer>> monitorAndVehicleSpeedWithVehicleNumDStream = processKey(monitorAndVehicleSpeedDStream);

		// <monitor_id, Tuple2<total_vehicle_speed, total_vehicle_num>>
		JavaPairDStream<String, Tuple2<Integer, Integer>> reduceByKeyAndWindow = getResult(monitorAndVehicleSpeedWithVehicleNumDStream);

		// print result
		printResult(reduceByKeyAndWindow);

		// 启动SparkStreaming
		jsc.start();
		jsc.awaitTermination();
	}

	private JavaDStream<String> vehicleLogDstream(JavaPairInputDStream<String, String> carRealTimeLogDStream) {
		JavaDStream<String> vehicleLogDStream = carRealTimeLogDStream.map(new Function<Tuple2<String, String>, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<String, String> tuple2) throws Exception {
				// new KeyedMessage<topic, key, value>
				// message key tuple2._1;
				// message value tuple2._2;
				// logger.error("message key : " + tuple2._1);
				return tuple2._2;
			}
		});
		return vehicleLogDStream;
	}

	private JavaDStream<String> filter(JavaDStream<String> vehicleLogDStream) {
		JavaDStream<String> vehicleLogFilterDStream = vehicleLogDStream.filter(new Function<String, Boolean>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String line) throws Exception {
				// 移除掉包含Common.ILLEGAL_LOG的行
				return !line.equals(Common.ILLEGAL_LOG);
			}
		});
		return vehicleLogFilterDStream;
	}

	private JavaPairDStream<String, Integer> monitorAndVehicleSpeedDstream(JavaDStream<String> vehicleLogFilterDStream) {
		JavaPairDStream<String, Integer> monitorAndVehicleSpeedDStream = vehicleLogFilterDStream.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String line) throws Exception {
				String[] split = line.split(Common.SEPARATOR);
				// <monitor_id, vehicle_speed>
				// 2019-01-22 18:15:18 SSX14139U 141 10006 20011 4002
				return new Tuple2<String, Integer>(split[4], Integer.valueOf(split[2]));
			}
		});
		return monitorAndVehicleSpeedDStream;
	}

	private JavaPairDStream<String, Tuple2<Integer, Integer>> processKey(JavaPairDStream<String, Integer> monitorAndVehicleSpeedDStream) {
		JavaPairDStream<String, Tuple2<Integer, Integer>> monitorAndVehicleSpeedWithVehicleNumDStream = monitorAndVehicleSpeedDStream.mapValues(new Function<Integer, Tuple2<Integer, Integer>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, Integer> call(Integer v) throws Exception {
				// <vehicle_speed, 1>
				return new Tuple2<Integer, Integer>(v, 1);
			}
		});
		return monitorAndVehicleSpeedWithVehicleNumDStream;
	}

	private JavaPairDStream<String, Tuple2<Integer, Integer>> getResult(JavaPairDStream<String, Tuple2<Integer, Integer>> monitorAndVehicleSpeedWithVehicleNumDStream) {
		JavaPairDStream<String, Tuple2<Integer, Integer>> reduceByKeyAndWindow = monitorAndVehicleSpeedWithVehicleNumDStream.reduceByKeyAndWindow(new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
				// <total_vehicle_speed, total_vehicle_num>
				// 加入新值
				return new Tuple2<Integer, Integer>(v1._1 + v2._1, v1._2 + v2._2());
			}
		}, new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
				// <total_vehicle_speed, total_vehicle_num>
				// 移除之前的值
				return new Tuple2<Integer, Integer>(v1._1 - v2._1, v2._2 - v2._2);
			}
			// 每隔Common.SPARK_STREAMING_PROCESS_DATA_FREQUENCY秒，处理过去Common.SPARK_STREAMING_PROCESS_DATA_HISTORY分钟的数据
		}, Durations.minutes(Common.SPARK_STREAMING_PROCESS_DATA_HISTORY), Durations.seconds(Common.SPARK_STREAMING_PROCESS_DATA_FREQUENCY));
		return reduceByKeyAndWindow;
	}

	/**
	 * output:
		[JAM        ] - Current Time : 2019-01-23 14:03:15, Monitor : 20051, Total Vehicle Number : 6, Total Vehicle Speed : 35, Current Average Speed : 5
		[GRIDLOCKED ] - Current Time : 2019-01-23 14:03:15, Monitor : 20098, Total Vehicle Number : 3, Total Vehicle Speed : 31, Current Average Speed : 10
		[SLOW-MOVING] - Current Time : 2019-01-23 14:03:15, Monitor : 20056, Total Vehicle Number : 9, Total Vehicle Speed : 218, Current Average Speed : 24
		[SLOW-MOVING] - Current Time : 2019-01-23 14:03:15, Monitor : 20081, Total Vehicle Number : 6, Total Vehicle Speed : 122, Current Average Speed : 20
	 */
	private void printResult(JavaPairDStream<String, Tuple2<Integer, Integer>> reduceByKeyAndWindow) {
		reduceByKeyAndWindow.foreachRDD(new VoidFunction<JavaPairRDD<String, Tuple2<Integer, Integer>>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaPairRDD<String, Tuple2<Integer, Integer>> rdd) throws Exception {
				final SimpleDateFormat sdf = new SimpleDateFormat(Common.DATE_FORMAT_YYYY_MM_DD_HHMMSS);
				logger.warn("**********************");
				rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Tuple2<Integer, Integer>>>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Iterator<Tuple2<String, Tuple2<Integer, Integer>>> ite) throws Exception {
						while (ite.hasNext()) {
							Tuple2<String, Tuple2<Integer, Integer>> tuple = ite.next();
							String monitorId = tuple._1;
							int totalVehicleSpeed = tuple._2._1;
							int totalVehicleNumber = tuple._2._2;
							// 如果vehicle speed < 30，就判断为拥堵状态
							if (totalVehicleNumber != 0) {
								int averageVehicleSpeed = totalVehicleSpeed / totalVehicleNumber;
								if (averageVehicleSpeed <= 5) {
									// 直接堵上了
									logger.warn("[JAM        ] - Current Time : " + sdf.format(Calendar.getInstance().getTime()) + ", Monitor : " + monitorId + ", Total Vehicle Number : " + totalVehicleNumber + ", Total Vehicle Speed : " + totalVehicleSpeed + ", Current Average Speed : " + averageVehicleSpeed);
								} else if (averageVehicleSpeed > 5 && averageVehicleSpeed <= 10) {
									// 严重拥堵，但是还可以移动
									logger.warn("[GRIDLOCKED ] - Current Time : " + sdf.format(Calendar.getInstance().getTime()) + ", Monitor : " + monitorId + ", Total Vehicle Number : " + totalVehicleNumber + ", Total Vehicle Speed : " + totalVehicleSpeed + ", Current Average Speed : " + averageVehicleSpeed);
								} else if (averageVehicleSpeed > 10 && averageVehicleSpeed <= 30) {
									// 行驶缓慢
									logger.warn("[SLOW-MOVING] - Current Time : " + sdf.format(Calendar.getInstance().getTime()) + ", Monitor : " + monitorId + ", Total Vehicle Number : " + totalVehicleNumber + ", Total Vehicle Speed : " + totalVehicleSpeed + ", Current Average Speed : " + averageVehicleSpeed);
								} else {
									// 正常通行
								}
							}
						}
					}
				});
			}
		});
	}

	/**
	 * 向Kafka中产生实时Vehicle Log数据
	 */
	private void generateDataToKafka() {
		logger.info("Begin Generate Data to Kafka");
		RealTimeDataGenerateUtils.generateDataToKafka();
	}

}
