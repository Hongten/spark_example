/**
 * Spark Example  
 * Mail: hongtenzone@foxmail.com 
 * Blog: http://www.cnblogs.com/hongten
 */
package com.hongten.spark.example.traffic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

import com.google.common.base.Optional;
import com.hongten.spark.example.traffic.datagenerate.Common;
import com.hongten.spark.example.traffic.datagenerate.util.DataLoadUtils;

/**
 * 当一辆车在道路上面行驶的时候，道路上面的监控点里面的摄像头就会对车进行数据采集。
 * 我们对采集的数据进行分析，处理。
 * 需求： 统计分析监控点和摄像头的状态（正常工作/异常）
 * 
 * @author Hongten
 * @created 20 Jan, 2019
 */
public class MonitorStatusMain {

	static final Logger logger = Logger.getLogger(MonitorStatusMain.class);

	public static void main(String[] args) {
		long begin = System.currentTimeMillis();
		processMonitorStatus();
		long end = System.currentTimeMillis();
		logger.info("Finished Task. Total: " + (end - begin) + " ms");
	}

	public static void processMonitorStatus() {
		SparkConf conf = new SparkConf();
		conf.setMaster(Common.MASTER_NAME).setAppName(Common.APP_NAME_MONITOR_STATUS);

		JavaSparkContext jsc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(jsc);

		// load test data
		loadData(jsc, sqlContext);

		// Road Monitor and Camera data process
		JavaPairRDD<String, List<String>> monitorAndCameraIdListRDD = roadMonitorAndCameraDataProcess(sqlContext);

		// Vehicle Log data process
		JavaPairRDD<String, Set<String>> vehicleLogMonitorAndCameraIdSetRDD = vehicleLogDataProcess(sqlContext);

		// Road Monitor and Camera data left outer join vehicle Log data
		JavaPairRDD<List<String>, List<String>> leftOuterJoinResultRDD = getLeftOuterJoinResultRDD(monitorAndCameraIdListRDD, vehicleLogMonitorAndCameraIdSetRDD);

		// print monitor status
		printMonitorStatus(leftOuterJoinResultRDD);
		
		jsc.stop();
	}

	private static JavaPairRDD<String, List<String>> roadMonitorAndCameraDataProcess(SQLContext sqlContext) {
		//生成对应的javaRDD
		JavaRDD<Row> roadMonitorAndCameraRDD = sqlContext.sql("select roadId, monitorId,cameraId from " + Common.T_ROAD_MONITOR_CAMERA_RELATIONSHIP).javaRDD();
		//把查询出来的一条一条数据组装成<monitor_id, Row>形式
		JavaPairRDD<String, Row> monitorAndRowRDD = roadMonitorAndCameraRDD.mapToPair(new PairFunction<Row, String, Row>() {

			private static final long serialVersionUID = 1L;

			// <monitor_id, Row>，这里也可以是<monitor_id, camera_id>的形式
			@Override
			public Tuple2<String, Row> call(Row row) throws Exception {
				return new Tuple2<String, Row>(row.getAs(Common.MONITOR_ID) + Common.EMPTY, row);
			}
		});

		// <monitor_id, Iterable<Row>>
		//通过groupByKey，这样就可以获取到同一个monitor_id下面对应的所有camera_id了
		JavaPairRDD<String, Iterable<Row>> monitorAndRowGroupByKeyRDD = monitorAndRowRDD.groupByKey();

		// <monirot_id, List<camera_id>>
		//再对groupByKey的结果进行组装，得到一个monitor_id对应一个List<camera_id>
		JavaPairRDD<String, List<String>> monitorAndCameraIdListRDD = monitorAndRowGroupByKeyRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, String, List<String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, List<String>> call(Tuple2<String, Iterable<Row>> tuple2) throws Exception {
				Iterator<Row> rows = tuple2._2.iterator();
				List<String> cameraIdList = new ArrayList<>();
				while (rows.hasNext()) {
					Row row = rows.next();
					cameraIdList.add(row.getAs(Common.CAMERA_ID) + Common.EMPTY);
				}
				//因为这里是主表，所以这里是可以使用List<String>
				return new Tuple2<String, List<String>>(tuple2._1, cameraIdList);
			}
		});
		return monitorAndCameraIdListRDD;
	}

	//这里的处理方式和roadMonitorAndCameraDataProcess()方法类似
	private static JavaPairRDD<String, Set<String>> vehicleLogDataProcess(SQLContext sqlContext) {
		//only query monitor_id and camera_id
		JavaRDD<Row> vehicleLogRDD = sqlContext.sql("select monitorId,cameraId from " + Common.T_VEHICLE_LOG).javaRDD();
		JavaPairRDD<String, Row> vehicleLogMonitorAndRowRDD = vehicleLogRDD.mapToPair(new PairFunction<Row, String, Row>() {

			private static final long serialVersionUID = 1L;

			// <monitor_id, Row>
			@Override
			public Tuple2<String, Row> call(Row row) throws Exception {
				return new Tuple2<String, Row>(row.getAs(Common.MONITOR_ID) + Common.EMPTY, row);
			}
		});

		JavaPairRDD<String, Iterable<Row>> vehicleLogMonitorAndRowGroupByKeyRDD = vehicleLogMonitorAndRowRDD.groupByKey();

		JavaPairRDD<String, Set<String>> vehicleLogMonitorAndCameraIdSetRDD = vehicleLogMonitorAndRowGroupByKeyRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, String, Set<String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Set<String>> call(Tuple2<String, Iterable<Row>> tuple2) throws Exception {
				Iterator<Row> rows = tuple2._2.iterator();
				// remove duplicate record
				//不同的vehicle 可能产生相同的camera_id记录
				//所以这里需要把相同的记录去掉(去重)
				Set<String> cameraIdSet = new HashSet<String>();
				while (rows.hasNext()) {
					Row row = rows.next();
					cameraIdSet.add(row.getAs(Common.CAMERA_ID) + Common.EMPTY);
				}
				return new Tuple2<String, Set<String>>(tuple2._1, cameraIdSet);
			}
		});
		return vehicleLogMonitorAndCameraIdSetRDD;
	}

	//把主表记录和Vehicle Log记录join起来
	//即在主表中拿到了<monitor_id, List<camera_id>>记录，并且在vehicle Log中也拿到了<monitor_id, Set<camera_id>>记录
	//这时需要把两个记录进行Left Outer Join
	//就会得到<monitor_id, Tuple2<List<camera_id>, Optional<Set<camera_id>>>>记录
	//即同一个monitor_id,拿主表的List<camera_id>记录和vehicle log中的Set<camera_id>记录进比较
	//vehicle log中的Set<camera_id>可能全都能匹配主表里面的List<camera_id> --> 说明该monitor_id工作正常
	//vehicle log中的Set<camera_id>可能不全都能匹配主表里面的List<camera_id> --> 说明该monitor_id异常
	private static JavaPairRDD<List<String>, List<String>> getLeftOuterJoinResultRDD(JavaPairRDD<String, List<String>> monitorAndCameraIdListRDD, JavaPairRDD<String, Set<String>> vehicleLogMonitorAndCameraIdSetRDD) {
		
		//left outer join
		JavaPairRDD<String, Tuple2<List<String>, Optional<Set<String>>>> leftOuterJoinRDD = monitorAndCameraIdListRDD.leftOuterJoin(vehicleLogMonitorAndCameraIdSetRDD);

		//cache left outer join result
		JavaPairRDD<String, Tuple2<List<String>, Optional<Set<String>>>> leftOuterJoinRDDCache = leftOuterJoinRDD.cache();
		
		JavaPairRDD<List<String>, List<String>> leftOuterJoinResultRDD = leftOuterJoinRDDCache.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, Tuple2<List<String>, Optional<Set<String>>>>>, List<String>, List<String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<List<String>, List<String>>> call(Iterator<Tuple2<String, Tuple2<List<String>, Optional<Set<String>>>>> ite) throws Exception {
				//存放所有异常的monitor_id
				List<String> errorMonitorIdList = new ArrayList<String>();
				//存放所有异常的camera_id
				List<String> errorCameraIdList = new ArrayList<String>();

				while (ite.hasNext()) {
					Tuple2<String, Tuple2<List<String>, Optional<Set<String>>>> tuple2 = ite.next();
					String monitorId = tuple2._1;
					List<String> stableCameraIdList = tuple2._2._1;
					Optional<Set<String>> optional = tuple2._2._2;
					Set<String> vehicleLogCameraIdSet = null;
					int stableCameraIdListSize = 0;
					int vehicleLogCameraIdSetSize = 0;

					// check if it has record
					if (optional.isPresent()) {
						vehicleLogCameraIdSet = optional.get();
						if (vehicleLogCameraIdSet != null) {
							vehicleLogCameraIdSetSize = vehicleLogCameraIdSet.size();
						}
					}

					if (stableCameraIdList != null) {
						stableCameraIdListSize = stableCameraIdList.size();
					}

					// this monitor with error
					if (stableCameraIdListSize != vehicleLogCameraIdSetSize) {
						if (vehicleLogCameraIdSetSize == 0) {
							// all cameras with error
							for (String cameraId : stableCameraIdList) {
								errorCameraIdList.add(cameraId);
							}
						} else {
							// at least one camera with error
							for (String cameraId : stableCameraIdList) {
								if (!vehicleLogCameraIdSet.contains(cameraId)) {
									errorCameraIdList.add(cameraId);
								}
							}
						}
						errorMonitorIdList.add(monitorId);
					}
				}

				return Arrays.asList(new Tuple2<List<String>, List<String>>(errorMonitorIdList, errorCameraIdList));
			}
		});
		return leftOuterJoinResultRDD;
	}

	/**
	 异常监控点id和异常监控点对应的摄像头id
	 output:
	 	Error monitor id : 20071
		Error monitor id : 20074
		Error monitor id : 20072
		Error monitor id : 20098
		Error monitor id : 20118
		Error camera id : 40141
		Error camera id : 40142
		Error camera id : 40147
		Error camera id : 40148
		Error camera id : 40143
		Error camera id : 40144
		Error camera id : 40195
		Error camera id : 40196
		Error camera id : 40235
		Error camera id : 40236
	 */
	private static void printMonitorStatus(JavaPairRDD<List<String>, List<String>> leftOuterJoinResultRDD) {
		leftOuterJoinResultRDD.foreach(new VoidFunction<Tuple2<List<String>, List<String>>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<List<String>, List<String>> tuple2) throws Exception {
				List<String> errorMonitorIdList = tuple2._1;
				List<String> errorCameraIdList = tuple2._2;

				if (errorMonitorIdList != null && errorMonitorIdList.size() > 0) {
					for (String monitorId : errorMonitorIdList) {
						logger.info("Error monitor id : " + monitorId);
					}

					if (errorCameraIdList != null && errorCameraIdList.size() > 0) {
						for (String cameraId : errorCameraIdList) {
							logger.info("Error camera id : " + cameraId);
						}
					}
				} else {
					logger.info("All monitors work well.");
				}
			}
		});
	}

	private static void loadData(JavaSparkContext jsc, SQLContext sqlContext) {
		DataLoadUtils.dataLoad(jsc, sqlContext, true);
	}

}
