/**
 * Spark Example  
 * Mail: hongtenzone@foxmail.com 
 * Blog: http://www.cnblogs.com/hongten
 */
package com.hongten.spark.example.streaming.datagenerate;

import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.hongten.spark.example.traffic.datagenerate.Common;
import com.hongten.spark.example.traffic.datagenerate.GenerateVehicleLog;
import com.hongten.spark.example.traffic.datagenerate.VehiclePlateGenerateSG;

/**
 * 产生实时数据线程类<br>
 * 
 * 该线程会每隔{@link Common.DATA_GENERATION_FREQUENCY}秒，<br>
 * 向Kafka中生产数据最大值为{@link Common.DATA_GENERATION_MAX_CAPACITY}的数据。<br>
 * 
 * @author Hongten
 * @created 22 Jan, 2019
 */
public class RealTimeDataGenerate implements Runnable {

	Integer[] roadIdArray;
	Random random;
	List<Integer> errorRoadIdList;
	Producer<String, String> producer;

	public RealTimeDataGenerate(Integer[] roadIdArray, Random random, List<Integer> errorRoadIdList) {
		super();
		this.roadIdArray = roadIdArray;
		this.random = random;
		this.errorRoadIdList = errorRoadIdList;
		producer = new Producer<String, String>(createProducerConfig());
	}

	@Override
	public void run() {
		int dataIndex = 0;
		while (true) {
			generateRealTimeDataToKafka();
			try {
				// 每次产生数据最大值
				if (dataIndex >= this.random.nextInt(Common.DATA_GENERATION_MAX_CAPACITY)) {
					// 数据产生频率(s)
					TimeUnit.SECONDS.sleep(Common.DATA_GENERATION_FREQUENCY);
					dataIndex = 0;
				} else {
					dataIndex++;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 向kafka中发送数据
	 */
	private void generateRealTimeDataToKafka() {
		producer.send(new KeyedMessage<String, String>(Common.KAFKA_TOPIC_SPARK_REAL_TIME_VEHICLE_LOG, getOneVehicleLog()));
	}

	/**
	 * 产生一条vehicle log<br>
	 * e.g.2019-01-22 18:15:18 SSX14139U 141 10006 20011 4002
	 */
	public String getOneVehicleLog() {
		String vehiclePlate = VehiclePlateGenerateSG.generatePlate(true);
		// 这里生成的vehicle log数据为实时数据，所以，data_time的format有所改变,并且vehiclePlate字段可以重复。其他字段保持不变。
		// 之前为：22/01/2019 18:15:18
		// 现在为：2019-01-22 18:15:18
		String oneVehicleLog = GenerateVehicleLog.getOneVehicleLog(true, Common.EMPTY, random, errorRoadIdList, vehiclePlate, roadIdArray);
		if (oneVehicleLog != null && oneVehicleLog.length() > 0) {
			oneVehicleLog = oneVehicleLog.substring(0, oneVehicleLog.length() - 1);
		} else {
			return Common.ILLEGAL_LOG;
		}
		return oneVehicleLog;
	}

	/**
	 * 配置Kafka
	 */
	public static ProducerConfig createProducerConfig() {
		Properties props = new Properties();
		props.put(Common.SERIALIZER_CLASS, Common.SERIALIZER_CLASS_AVLUE);
		props.put(Common.METADATA_BROKER_LIST, Common.METADATA_BROKER_LIST_VALUE);
		return new ProducerConfig(props);
	}

}
