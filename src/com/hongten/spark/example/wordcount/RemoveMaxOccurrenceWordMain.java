/**
 * Spark Example  
 * Mail: hongtenzone@foxmail.com 
 * Blog: http://www.cnblogs.com/hongten
 */
package com.hongten.spark.example.wordcount;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

import com.hongten.spark.example.traffic.datagenerate.Common;

/**
 * 需求： 读取一个文件，统计单词出现的次数。并且把出现次数多的单词移除掉，然后将剩下的单词按照出现次数降序排序。
 * 
 * @author Hongten
 * @created 24 Jan, 2019
 */
public class RemoveMaxOccurrenceWordMain implements Serializable {

	private static final long serialVersionUID = 1L;

	static final Logger logger = Logger.getLogger(RemoveMaxOccurrenceWordMain.class);

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster(Common.MASTER_NAME).setAppName("Remove More Occurrence Word");

		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaRDD<String> lines = loadFile(jsc);

		// 获取一个一个的单词
		JavaRDD<String> wordsRDD = lines.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String line) throws Exception {
				return Arrays.asList(line.split(Common.BLANK));
			}
		});

		// 给每个单词赋值1，返回<word, 1>形式
		JavaPairRDD<String, Integer> wordWithValueRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				// <word, 1>
				return new Tuple2<String, Integer>(word, 1);
			}
		});

		// 对每个单词进行统计
		JavaPairRDD<String, Integer> wordWithTotalNumRDD = wordWithValueRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});

		// 把<word, totalNum>形式，交换位置为<totalNum, word>
		JavaPairRDD<Integer, String> totalNumAndWordRDD = wordWithTotalNumRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
				// <totalNum, word>
				return tuple2.swap();
			}
		});

		// 对<totalNum, word>形式进行降序排序
		JavaPairRDD<Integer, String> totalNumAndWordSortByKeyRDD = totalNumAndWordRDD.sortByKey(false);

		// 这里把排序好的结果放入Cache，方便后面的计算
		JavaPairRDD<Integer, String> totalNumAndWordSortByKeyCachedRDD = totalNumAndWordSortByKeyRDD.cache();

		// 获取第一个，即出现次数最多的那个单词
		List<Tuple2<Integer, String>> takeOne = totalNumAndWordSortByKeyCachedRDD.take(1);

		// 这个是就是出现次数最多的单词
		final String oneWord = takeOne.get(0)._2;
		Integer oneWordTotalNum = takeOne.get(0)._1;

		/**
		 * output: 
		 * Find the Max Occurrence Word : a, Number : 6
		 */
		logger.info("Find the Max Occurrence Word : " + oneWord + ", Number : " + oneWordTotalNum);

		// 我们之前对所有单词以及他们出现的次数进行了降序排序
		// 那么，我们现在只需要把结果中，移除掉出现次数最多的单词即可
		JavaPairRDD<Integer, String> resultRDD = totalNumAndWordSortByKeyCachedRDD.filter(new Function<Tuple2<Integer, String>, Boolean>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<Integer, String> tuple2) throws Exception {
				// 移除掉出现次数最多的单词
				return !tuple2._2.equals(oneWord);
			}
		});

		/**
		 * output: 
		 	(is,3)
			(Spark,2)
			(Apache,2)
			(data,2)
			(for,2)
			(and,2)
			(engine,2)
			......
		 */
		// 打印结果
		resultRDD.foreach(new VoidFunction<Tuple2<Integer, String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<Integer, String> tuple2) throws Exception {
				logger.info(tuple2.swap());
			}
		});

		jsc.stop();
	}

	private static JavaRDD<String> loadFile(JavaSparkContext jsc) {
		JavaRDD<String> lines = jsc.textFile("./resources/remove_max_occurrence_test_data.txt");
		return lines;
	}
}
