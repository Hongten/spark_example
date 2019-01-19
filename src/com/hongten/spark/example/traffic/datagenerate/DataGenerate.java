package com.hongten.spark.example.traffic.datagenerate;

import java.io.File;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.hongten.spark.example.traffic.datagenerate.util.FileUtils;
import com.hongten.spark.example.traffic.datagenerate.vo.VehicleLogVO;

/**
 * 产生测试数据：
 * 数据format：
 * 记录时间    	         车牌号码    		车速			         道路编号       监控地点		摄像头编号
 * date_time 	vehicle_plate 	vehicle_speed	road_id   monitor_id	camera_id
 * 
 * 中间使用'\t'隔开
 * 16/01/2019 10:20:30 SCN89000J 124 10002 20004 40007
 * 
 * 具体说明：
 * 道路编号
 * 10001 - 10100
 * 
 * 监控地点 - 在一条道路上面有2个监控点
 * 20001 - 20200
 * 
 * 摄像头编号 - 在一个监控点上面2个摄像头
 * 40001 - 40400
 * 
 * 道路：      10001                         10002
 * 监控：      20001-20002                   20003-20004
 * 摄像头：  40001-40002-40003-40004       40005-40006-40007-40008
 * 
 * 车速： 1-300。 如果大于260，则为超速行驶
 * 
 * 车牌： SCN89000J
 * 
 * 记录时间： 16/01/2019 10:20:30 
 * 
 * @author Hongten
 */
public class DataGenerate implements DataLoad, Serializable{
	
	private static final long serialVersionUID = -4376164446456994791L;
	static final Logger logger = Logger.getLogger(DataGenerate.class);

	Integer[] roadIdArray = new Integer[Common.ROAD_NUM];
	//是否要把生成的数据写入到文件保存
	//默认是写入文件保存的
	boolean isGenerateFile = true;
	
	public DataGenerate(boolean isGenerateFile){
		this.isGenerateFile = isGenerateFile;
	}
	
	@Override
	public void dataLoadFromFile(JavaSparkContext jsc, SQLContext sqlContext) {
		File file = new File(Common.VEHICLE_LOG);
		if (file.exists() && !isGenerateFile) {
			loadRoadMonitorAndCameraDataFromFile(jsc, sqlContext);
			loadVehicleLogDataFromFile(jsc, sqlContext);
		} else {
			// 如果文件不存在，那么就在载入data的同时，把data写入到文件中，供下次使用
			isGenerateFile = true;
			loadDataFromAutoGenerate(jsc, sqlContext);
		}
	}
	
	@Override
	public void loadDataFromAutoGenerate(JavaSparkContext jsc, SQLContext sqlContext) {
		init(jsc, sqlContext);
		generateData(jsc, sqlContext);
	}

	private void loadVehicleLogDataFromFile(JavaSparkContext jsc, SQLContext sqlContext) {
		JavaRDD<String> vehicleLogLines = jsc.textFile(Common.VEHICLE_LOG);
		JavaRDD<VehicleLogVO> vehicleLogRDD = vehicleLogLines.map(new Function<String, VehicleLogVO>() {

			private static final long serialVersionUID = 1L;

			@Override
			public VehicleLogVO call(String line) throws Exception {
				String[] split = line.split(Common.SEPARATOR);
				VehicleLogVO vo = new VehicleLogVO(split[0], split[1], Integer.valueOf(split[2]), split[3], split[4], split[5]);
				return vo;
			}
		});

		DataFrame vehicleLogDataFrame = sqlContext.createDataFrame(vehicleLogRDD, VehicleLogVO.class);
		vehicleLogDataFrame.registerTempTable(Common.T_VEHICLE_LOG);
		logger.info("------------ Finished load Vehicle Log data! --------------");
		//sqlContext.sql("select count(*) from " + Common.T_VEHICLE_LOG).show();
	}

	private void loadRoadMonitorAndCameraDataFromFile(JavaSparkContext jsc, SQLContext sqlContext) {
		JavaRDD<String> lines = jsc.textFile(Common.ROAD_MONITOR_CAMERA_RELATIONSHIP);
		JavaRDD<VehicleLogVO> vehicleLogVORDD = lines.map(new Function<String, VehicleLogVO>() {

			private static final long serialVersionUID = 1L;

			@Override
			public VehicleLogVO call(String line) throws Exception {
				String[] split = line.split(Common.SEPARATOR);
				VehicleLogVO vo = new VehicleLogVO(split[0], split[1], split[2]);
				return vo;
			}
		});

		DataFrame rowDataFrame = sqlContext.createDataFrame(vehicleLogVORDD, VehicleLogVO.class);
		rowDataFrame.registerTempTable(Common.T_ROAD_MONITOR_CAMERA_RELATIONSHIP);

		logger.info("------------ Finished load Road Monitor and Camera relationship data! ------------");
		//sqlContext.sql("select count(*) from " + Common.T_ROAD_MONITOR_CAMERA_RELATIONSHIP).show();
	}

	public void init(JavaSparkContext jsc, SQLContext sqlContext) {
		// create files
		if (isGenerateFile) {
			FileUtils.createFile(Common.VEHICLE_LOG);
			FileUtils.createFile(Common.ROAD_MONITOR_CAMERA_RELATIONSHIP);
			FileUtils.createFile(Common.ERROR_ROAD_IDS);
		}
		generateRoadIds(jsc, sqlContext);
	}

	/**
	 * 道路： 10001 10002 
	 * 监控： 20001-20002 20003-20004 
	 * 摄像头： 40001-40002-40003-40004 40005-40006-40007-40008
	 */
	public void generateRoadIds(JavaSparkContext jsc, SQLContext sqlContext) {
		List<Row> dataList = new ArrayList<Row>();
		StringBuilder readMonitorCameraRelationship = null;
		if (isGenerateFile) {
			readMonitorCameraRelationship = new StringBuilder();
		}
		for (int i = 0; i < Common.ROAD_NUM; i++) {
			int roadId = 10000 + (i + 1);// 10001
			roadIdArray[i] = roadId;

			int monitorB = roadId * 2;// 20002
			int monitorA = monitorB - 1;// 20001

			int cameraAB = monitorA * 2;// 40002
			int cameraAA = cameraAB - 1;// 40001

			int cameraBB = monitorB * 2;// 40004
			int cameraBA = cameraBB - 1;// 40003

			if (isGenerateFile) {
				roadMonitorAndCameraRelationshipData(readMonitorCameraRelationship, roadId, monitorB, monitorA, cameraAB, cameraAA, cameraBB, cameraBA);
			}
			dataList.add(RowFactory.create(roadId+Common.EMPTY, monitorA+Common.EMPTY, cameraAA+Common.EMPTY));
			dataList.add(RowFactory.create(roadId+Common.EMPTY, monitorA+Common.EMPTY, cameraAB+Common.EMPTY));
			dataList.add(RowFactory.create(roadId+Common.EMPTY, monitorB+Common.EMPTY, cameraBA+Common.EMPTY));
			dataList.add(RowFactory.create(roadId+Common.EMPTY, monitorB+Common.EMPTY, cameraBB+Common.EMPTY));
		}
		if (isGenerateFile) {
			saveData(Common.ROAD_MONITOR_CAMERA_RELATIONSHIP, readMonitorCameraRelationship.toString());
		}

		StructType roadMonitorAndCameraSctrucTyps = DataTypes.createStructType(Arrays.asList(DataTypes.createStructField(Common.ROAD_ID, DataTypes.StringType, true), DataTypes.createStructField(Common.MONITOR_ID, DataTypes.StringType, true), DataTypes.createStructField(Common.CAMERA_ID, DataTypes.StringType, true)));

		JavaRDD<Row> roadMonitorAndCameraRDD = jsc.parallelize(dataList);
		DataFrame roadMonitorAndCameraDataFrame = sqlContext.createDataFrame(roadMonitorAndCameraRDD, roadMonitorAndCameraSctrucTyps);
		roadMonitorAndCameraDataFrame.registerTempTable(Common.T_ROAD_MONITOR_CAMERA_RELATIONSHIP);
		logger.info("Finished load Road Monitor and Camera relationship data! Total records : " + (Common.ROAD_NUM * 4));
	}

	public void generateData(JavaSparkContext jsc, SQLContext sqlContext) {
		
		List<Row> dataList = new ArrayList<Row>();
		
		//StringBuffer可以保证线程安全
		StringBuffer contentSb = null;
		if(isGenerateFile){
			contentSb = new StringBuffer();
		}
		SimpleDateFormat simpleDateFormat_ddMMyyyy = new SimpleDateFormat(Common.DATE_FORMAT_YYYYMMDD);
		Date today = new Date();
		String date = simpleDateFormat_ddMMyyyy.format(today);
		Random random = new Random();
		
		//异常道路
		List<Integer> errorRoadIdList = new ArrayList<Integer>();
		generateErrorRoadIdList(random, errorRoadIdList);
		String errorRoadIdsString = "";
		for (int x = 0; x < Common.ROAD_ERROR_NUM; x++) {
			errorRoadIdsString +=  errorRoadIdList.get(x) + Common.LINE_BREAK;
			logger.info("Error Road ID = " + errorRoadIdList.get(x));
		}
		
		if(isGenerateFile && errorRoadIdsString.length() > 0){
			saveData(Common.ERROR_ROAD_IDS, errorRoadIdsString);
		}
		
		long begin = System.currentTimeMillis();
		
		//使用多线程
		ExecutorService exec = Executors.newCachedThreadPool();
		for (int i = 0; i < Common.VEHICLE_NUMBER; i++) {
			String vehiclePlate = VehiclePlateGenerateSG.generatePlate();
			//使用Future和Callable组合，可以获取到返回值
			Future<String> result = exec.submit(new GenerateVehicleLog(date, random, errorRoadIdList, vehiclePlate, roadIdArray));
			try {
				String resultContent = result.get();
				if(isGenerateFile){
					contentSb.append(resultContent);
				}

				if (resultContent != null && resultContent.length() > 0) {
					String[] lines = resultContent.split(Common.LINE_BREAK);
					if (lines != null && lines.length > 0) {
						for (String line : lines) {
							String[] datas = line.split(Common.SEPARATOR);
							if (datas != null && datas.length == 6) {
								// date_time vehicle_plate vehicle_speed road_id monitor_id camera_id
								dataList.add(RowFactory.create(datas[0], datas[1], datas[2], datas[3], datas[4], datas[5]));
							}
						}
					}
				}
				
				if(i != 0 && i % 900 == 0){
					long end = System.currentTimeMillis();
					logger.info(i + " sleeping 1 seconds." + " " + (end - begin)/1000 + " s");
					//waiting the pre-task to finish.
					TimeUnit.SECONDS.sleep(1);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
		exec.shutdown();
		if(isGenerateFile){
			saveData(Common.VEHICLE_LOG, contentSb.toString());
		}
		
		StructType vehicleLogStructType = DataTypes.createStructType(Arrays.asList(DataTypes.createStructField(Common.DATE_TIME, DataTypes.StringType, true), DataTypes.createStructField(Common.VEHICLE_PLATE, DataTypes.StringType, true), DataTypes.createStructField(Common.VEHICLE_SPEED, DataTypes.IntegerType, true), DataTypes.createStructField(Common.ROAD_ID, DataTypes.StringType, true), DataTypes.createStructField(Common.MONITOR_ID, DataTypes.StringType, true), DataTypes.createStructField(Common.CAMERA_ID, DataTypes.StringType, true)));

		JavaRDD<Row> vehicleLogRDD = jsc.parallelize(dataList);
		DataFrame vehicleLogDataFrame = sqlContext.createDataFrame(vehicleLogRDD, vehicleLogStructType);
		vehicleLogDataFrame.registerTempTable(Common.T_VEHICLE_LOG);
		logger.info("Finished load Vehicle Log data! Vehicle Log records : " + ((dataList != null && dataList.size() > 0) ? dataList.size(): 0));
	}

	private void generateErrorRoadIdList(Random random, List<Integer> errorRoadIdList) {
		for (int x = 0; x < Common.ROAD_ERROR_NUM; x++) {
			if (errorRoadIdList.contains(roadIdArray[random.nextInt(roadIdArray.length)])) {
				generateErrorRoadIdList(random, errorRoadIdList);
			} else {
				errorRoadIdList.add(roadIdArray[random.nextInt(roadIdArray.length)]);
			}
		}
	}
	
	private void roadMonitorAndCameraRelationshipData(StringBuilder readMonitorCameraRelationship, int roadId, int monitorB, int monitorA, int cameraAB, int cameraAA, int cameraBB, int cameraBA) {
		// monitorA
		// 10001 20001 40001
		// 10001 20001 40002
		readMonitorCameraRelationship.append(roadId).append(Common.SEPARATOR).append(monitorA).append(Common.SEPARATOR).append(cameraAA).append(Common.LINE_BREAK);
		readMonitorCameraRelationship.append(roadId).append(Common.SEPARATOR).append(monitorA).append(Common.SEPARATOR).append(cameraAB).append(Common.LINE_BREAK);
		// monitorB
		// 10001 20002 40003
		// 10001 20002 40004
		readMonitorCameraRelationship.append(roadId).append(Common.SEPARATOR).append(monitorB).append(Common.SEPARATOR).append(cameraBA).append(Common.LINE_BREAK);
		readMonitorCameraRelationship.append(roadId).append(Common.SEPARATOR).append(monitorB).append(Common.SEPARATOR).append(cameraBB).append(Common.LINE_BREAK);
	}

	public void saveData(String pathFileName, String newContent) {
		//remove the last '\n'
		newContent = newContent.substring(0, newContent.length() - 1);
		FileUtils.saveFile(pathFileName, newContent);
	}

}
