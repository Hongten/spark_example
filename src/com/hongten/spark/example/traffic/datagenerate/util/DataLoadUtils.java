/**
 * Spark Example  
 * Mail: hongtenzone@foxmail.com 
 * Blog: http://www.cnblogs.com/hongten
 */
package com.hongten.spark.example.traffic.datagenerate.util;

import java.io.Serializable;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import com.hongten.spark.example.traffic.datagenerate.DataGenerate;
import com.hongten.spark.example.traffic.datagenerate.DataLoad;

/**
 * @author Hongten
 * @created 20 Jan, 2019
 */
public class DataLoadUtils implements Serializable{

	private static final long serialVersionUID = 1L;

	public static void dataLoad(JavaSparkContext jsc, SQLContext sqlContext, boolean loadRoadMonitorAndCameraData) {
		// this data will be auto-generated when loading
		DataLoad dataLoad = new DataGenerate(false);
		dataLoad.dataLoadFromFile(jsc, sqlContext, loadRoadMonitorAndCameraData);
	}
}
