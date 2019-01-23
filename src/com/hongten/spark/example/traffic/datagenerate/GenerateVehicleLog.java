/**
 * Spark Example  
 * Mail: hongtenzone@foxmail.com 
 * Blog: http://www.cnblogs.com/hongten
 */
package com.hongten.spark.example.traffic.datagenerate;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

import com.hongten.spark.example.traffic.datagenerate.util.StringUtils;

/**
 * @author Hongten
 * @created 20 Jan, 2019
 */
public class GenerateVehicleLog implements Callable<String>{

	StringBuffer contentSb;
	String date;
	Random random;
	List<Integer> errorRoadIdList;
	String vehiclePlate;
	Integer[] roadIdArray;
	static SimpleDateFormat simpleDateFormat = new SimpleDateFormat(Common.DATE_FORMAT_YYYY_MM_DD_HHMMSS);
	static int[] seed = new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 50, 60, 100, 150, 300 };

	public GenerateVehicleLog() {
		super();
	}

	public GenerateVehicleLog( String date, Random random, List<Integer> errorRoadIdList, String vehiclePlate, Integer[] roadIdArray){
		this.contentSb = new StringBuffer();
		this.date = date;
		this.random = random;
		this.errorRoadIdList = errorRoadIdList;
		this.vehiclePlate = vehiclePlate;
		this.roadIdArray = roadIdArray;
	}
	
	@Override
	public String call() throws Exception {
		return getVehicleLog(contentSb, date, random, errorRoadIdList, vehiclePlate, roadIdArray);
	}
	
	private String getVehicleLog(StringBuffer contentSb, String date, Random random, List<Integer> errorRoadIdList, String vehiclePlate, Integer[] roadIdArray) {
		// 每一辆车产生记录在100条记录以内
		// 即最多过100个监控点(Common.ROAD_NUM*2)
		// 即最多过50条路(Common.ROAD_NUM)
		// 这里可以根据需要调节
		for (int n = 0; n < random.nextInt(Common.ROAD_NUM); n++) {
			contentSb.append(getOneVehicleLog(false, date, random, errorRoadIdList, vehiclePlate, roadIdArray));
		}
		return contentSb.toString();
	}

	public static String getOneVehicleLog(boolean isRealTime, String date, Random random, List<Integer> errorRoadIdList, String vehiclePlate, Integer[] roadIdArray) {
		StringBuffer contentSb = new StringBuffer();
		int roadId = roadIdArray[random.nextInt(roadIdArray.length)];
		Integer[] monitorIdArray = new Integer[2];
		Integer[] cameraIdArray = new Integer[2];
		boolean isAllError = false;
		int monitorId = 0;
		if (errorRoadIdList.contains(roadId)) {
			// System.out.println("find error road.... " + roadId +
			// " for vehicle : " + vehiclePlate);
			if (roadId % 2 == 0) {
				// 监控设备全部坏掉
				isAllError = true;
			} else {
				// 部分坏掉
				monitorIdArray[0] = roadId * 2 - 1;
				monitorIdArray[1] = roadId * 2 - 1;

				monitorId = monitorIdArray[random.nextInt(monitorIdArray.length)];

				cameraIdArray[0] = roadId * 4 - 3;
				cameraIdArray[1] = roadId * 4 - 2;
			}
		} else {
			monitorIdArray[0] = roadId * 2 - 1;
			monitorIdArray[1] = roadId * 2;

			monitorId = monitorIdArray[random.nextInt(monitorIdArray.length)];

			cameraIdArray[0] = monitorId * 2 - 1;
			cameraIdArray[1] = monitorId * 2;
		}

		if (!isAllError) {
			//实时数据和之前的数据的date_time格式稍有变化，其他字段保持不变
			if (isRealTime) {
				// 2019-01-16 10:20:30 SCN89000J 124 10002 20004 40007
				contentSb.append(simpleDateFormat.format(new Date())).append(Common.SEPARATOR);
			} else {
				// 16/01/2019 10:20:30 SCN89000J 124 10002 20004 40007
				contentSb.append(date).append(Common.BLANK).append(StringUtils.fulfuill(String.valueOf(random.nextInt(24)))).append(Common.COLON).append(StringUtils.fulfuill(String.valueOf(random.nextInt(60)))).append(Common.COLON).append(StringUtils.fulfuill(String.valueOf(random.nextInt(60)))).append(Common.SEPARATOR);
			}
			contentSb.append(vehiclePlate).append(Common.SEPARATOR);
			//为了模拟真实的车速
			int nextInt = seed[random.nextInt(seed.length)];
			contentSb.append((random.nextInt(Common.MAX_SPEED)/nextInt + 1)).append(Common.SEPARATOR);
			contentSb.append(roadId).append(Common.SEPARATOR);
			contentSb.append(monitorId).append(Common.SEPARATOR);
			contentSb.append(cameraIdArray[random.nextInt(cameraIdArray.length)]).append(Common.LINE_BREAK);
		}
		return contentSb.toString();
	}
}