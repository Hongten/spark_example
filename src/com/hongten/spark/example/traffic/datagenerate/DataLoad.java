/**
 * Spark Example  
 * Mail: hongtenzone@foxmail.com 
 * Blog: http://www.cnblogs.com/hongten
 */
package com.hongten.spark.example.traffic.datagenerate;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * @author Hongten
 * @created 20 Jan, 2019
 */
public interface DataLoad {

	public void dataLoadFromFile(JavaSparkContext jsc, SQLContext sqlContext);

	public void loadDataFromAutoGenerate(JavaSparkContext jsc, SQLContext sqlContext);
}
