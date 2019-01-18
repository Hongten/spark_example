package com.hongten.spark.example.wordcount;

import java.util.Arrays;
import java.util.List;

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

/**
 * 对单词个数进行统计，然后按照出现次数由大到小输出。
 * 
 * @useage: WordCount
 * 
 * @author Hongten
 */
public class WordCount {

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		long begin = System.currentTimeMillis();
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("wordCount");

		// JavaSparkContext是通往集群的唯一通道
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setCheckpointDir("./resource/checkpointDir");
		// 读取文件
		JavaRDD<String> lines = sc.textFile("./resources/word_count_test_data.txt");
		
		//添加filter
		JavaRDD<String> filterLines = lines.filter(
		/**
		 * String --读取Lines里面的的一行数据 
		 * Boolean --
		 */
		new Function<String, Boolean>() {

			private static final long serialVersionUID = -7222805379226673309L;

			//包含有"Hello"和"Hello1"的行
			@Override
			public Boolean call(String line) throws Exception {
				return line.contains("Hello") || line.contains("Hello1");
			}
		});
		
		//抽样
		//true -- 放回抽样
	    //false --不放回抽样
	    //0.2 --抽样比例
	    //seed --针对同一批数据，只要seed相同，每次抽样的数据集一样
		//JavaRDD<String> sampleLines = filterLines.sample(true, 0.2, 50);
		
		// RDD的持久化
		// cache把数据存在内存中
		//JavaRDD<String> cacheData = sampleLines.cache();
		
		List<String> take1 = filterLines.take(2);
		//first = take1
		String first = filterLines.first();
		
		//same
		System.out.println("take 1 : " + take1.get(0));
		System.out.println("first : " + first);
		
		/**
		 * output:
		 * 
		 * take 1 : Hello	Hongten	40001
		 * first : Hello	Hongten	40001
		 */
		
		// 以空格切单词
		//一对多的关系，进来一行数据，输出多个单词
		JavaRDD<String> words = filterLines.flatMap(

		/**
		 * String1 --读取Lines里面的的一行数据 
		 * String2 --输出数据类型，即集合里面的数据类型
		 */
		new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			/**
			 * @param line 就是String1，读取的一行数据
			 * @return 返回一个数组
			 */
			@Override
			public Iterable<String> call(String line) throws Exception {
				return Arrays.asList(line.split("\t"));
			}
		});

		// 对单词进行计数
		//example: <"Hello", 1>, <"Hongten", 1>
		JavaPairRDD<String, Integer> pairWords = words.mapToPair(
		/**
		 * String1 --一个单词 
		 * String2 --Tuple2元组里面的key 
		 * Integer --Tuple2元组里面的value
		 */
		new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = -2962565084610481112L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		
		//合并结果
		JavaPairRDD<String, Integer> reduce = pairWords.reduceByKey(

		/**
		 * Integer1 --进来的数据<"Hello", 1>中的1
		 * Integer2 --进来的数据<"Hello", 1>中的1
		 * Integer3 --输出结果
		 * 
		 * 1.先将相同的key分组
		 * 2.对每一组的key对应的value去按照你的逻辑处理
		 */
		new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		
		//排序
		//输入： <"Hello", 23>
		//输出： <23, "Hello">
		JavaPairRDD<Integer, String> result = reduce.mapToPair(
		/**
		 * String1 --进来的一条结果
		 * String2 --输出Tuple2元组里面的key 
		 * Integer --输出Tuple2元组里面的value
		 */
		new PairFunction<Tuple2<String, Integer>, Integer, String>() {

			private static final long serialVersionUID = -5424248599737135001L;

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple)
					throws Exception {
				//return new Tuple2<Integer, String>(tuple._2, tuple._1);
				return tuple.swap();
			}
		});
		
		//默认升序排序true
		JavaPairRDD<Integer, String> sortByKeyResult = result.sortByKey(false);
		
		//打印结果
		//输入：  <23, "Hello">
		sortByKeyResult.foreach(new VoidFunction<Tuple2< Integer, String>>() {

			private static final long serialVersionUID = 2496749255383918020L;

			@Override
			public void call(Tuple2<Integer, String> tuple) throws Exception {
				//System.out.println(tuple._1 + " - " + tuple._2);
				System.out.println(tuple.swap());
			}
		});
		
		
		/**
		 * output:
		    (Hello,19)
			(Hello1,4)
			(20002,2)
			(20006,2)
			(20008,2)
			(20004,2)
			(20012,2)
			(20011,2)
			(20007,2)
			(20003,2)
			(20009,2)
			(20005,2)
			(this,1)
			(40018,1)
			(40007,1)
			(is,1)
			(40013,1)
			(40010,1)
			(40006,1)
			(20010,1)
			(40003,1)
			(40002,1)
			(40005,1)
			(data,1)
			(20001,1)
			(40023,1)
			(40008,1)
			(40015,1)
			(word,1)
			(40011,1)
			(40014,1)
			(40004,1)
			(40024,1)
			(40012,1)
			(40001,1)
			(40016,1)
			(test,1)
			(Hongten,1)
		 */
		
		
		
		
		sortByKeyResult.cache();
		sortByKeyResult.checkpoint();
		
		//统计
		long count = sortByKeyResult.count();
		System.out.println("Count : " + count);
		
		/**
		 * output:
		 *  Count : 38
		 */
		
		//将worker端的结果回收到Driver端
	    //如果结果很大，会导致Driver端内存溢出OOM
		List<Tuple2<Integer, String>> collectResult = sortByKeyResult.collect();
		for (Tuple2<Integer, String> tem : collectResult) {
			System.out.println("collect : " + tem.swap());
		}
		
		/**
		 * output:
		 	collect : (Hello,19)
			collect : (Hello1,4)
			collect : (20002,2)
			collect : (20006,2)
			collect : (20008,2)
			collect : (20004,2)
			collect : (20012,2)
			collect : (20011,2)
			collect : (20007,2)
			collect : (20003,2)
			collect : (20009,2)
			collect : (20005,2)
			collect : (this,1)
			collect : (40018,1)
			collect : (40007,1)
			collect : (is,1)
			collect : (40013,1)
			collect : (40010,1)
			collect : (40006,1)
			collect : (20010,1)
			collect : (40003,1)
			collect : (40002,1)
			collect : (40005,1)
			collect : (data,1)
			collect : (20001,1)
			collect : (40023,1)
			collect : (40008,1)
			collect : (40015,1)
			collect : (word,1)
			collect : (40011,1)
			collect : (40014,1)
			collect : (40004,1)
			collect : (40024,1)
			collect : (40012,1)
			collect : (40001,1)
			collect : (40016,1)
			collect : (test,1)
			collect : (Hongten,1)
		 */
		
		sc.stop();
		long end = System.currentTimeMillis();
		System.out.println("total: " + (end - begin)/1000 + " s");
		
		/**
		 * output:
		 *  total: 5 s
		 */
	}
	
}