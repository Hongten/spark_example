package com.hongten.spark.example.traffic.datagenerate;

import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

import com.hongten.spark.example.traffic.datagenerate.util.StringUtils;

class GenerateVehicleLog implements Callable<String>{

	StringBuffer contentSb;
	String date;
	Random random;
	List<Integer> errorRoadIdList;
	String vehiclePlate;
	Integer[] roadIdArray;
	
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
				// 16/01/2019 10:20:30 SCN89000J 124 10002 20004 40007
				contentSb.append(date).append(Common.BLANK).append(StringUtils.fulfuill(String.valueOf(random.nextInt(25)))).append(Common.COLON).append(StringUtils.fulfuill(String.valueOf(random.nextInt(61)))).append(Common.COLON).append(StringUtils.fulfuill(String.valueOf(random.nextInt(61)))).append(Common.SEPARATOR);
				contentSb.append(vehiclePlate).append(Common.SEPARATOR);
				contentSb.append((random.nextInt(Common.MAX_SPEED) + 1)).append(Common.SEPARATOR);
				contentSb.append(roadId).append(Common.SEPARATOR);
				contentSb.append(monitorId).append(Common.SEPARATOR);
				contentSb.append(cameraIdArray[random.nextInt(cameraIdArray.length)]).append(Common.LINE_BREAK);
			}
		}
		return contentSb.toString();
	}
}