package com.hongten.spark.example.traffic.datagenerate;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

public interface DataLoad {

	public void dataLoadFromFile(JavaSparkContext jsc, SQLContext sqlContext);

	public void loadDataFromAutoGenerate(JavaSparkContext jsc, SQLContext sqlContext);
}
