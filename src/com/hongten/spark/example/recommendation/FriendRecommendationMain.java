/**
 * Mail: hongtenzone@foxmail.com 
 * Blog: http://www.cnblogs.com/hongten
 */
package com.hongten.spark.example.recommendation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

import com.hongten.spark.example.traffic.datagenerate.Common;

/**
 * 
 * 需求：实现 好友推荐功能,推荐Top2的好友
 * 
 * 目的：增加application的用户粘度，让更多的用户使用该产品
 * 
 * 
 * 人物关系图 ：  https://github.com/Hongten/spark_example/blob/master/resources/recommendation.png
 * 
 * A B C G
 * B A E
 * C A D E
 * D C E G
 * E B C D F G
 * F E G
 * G A D E F
 * 
 * 数据分析：
 * A B C G
 * A有朋友B,C,G
 * 那么A-B,A-C,A-G都是属于直接朋友关系
 * 如果单看这一行数据，那么B-C,B-G,C-G都是属于间接朋友关系
 * 
 * B A E
 * B有朋友A,E
 * 那么B-A,B-E都是属于直接朋友关系
 * 
 * 这时候，我们就可以看出A-E属于间接朋友关系，因为他们之间有B
 * 我们要实现的功能就是：把A推荐给E，或把E推荐给A
 * 
 * 
 * @author Hongten
 * @created 5 Apr, 2019
 */
public class FriendRecommendationMain implements Serializable {

	private static final long serialVersionUID = 1L;

	static final Logger logger = Logger.getLogger(FriendRecommendationMain.class);

	public static final int TOP_N = 2;

	public static void main(String[] args) {
		long begin = System.currentTimeMillis();
		FriendRecommendationMain friendRecommendationMain = new FriendRecommendationMain();
		friendRecommendationMain.processFriendREcommendation();
		long end = System.currentTimeMillis();
		logger.info("Finished Task. Total: " + (end - begin) + " ms");
	}

	/**
	 * 
	 */
	private void processFriendREcommendation() {
		SparkConf conf = new SparkConf();
		conf.setMaster(Common.MASTER_NAME).setAppName("Friend Recommendation");

		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaRDD<String> lines = jsc.textFile("./resources/recommendation_data");

		// input: A B C G
		// output: A-B-0, B-C-1
		JavaRDD<String> flatMapRDD = flatMapRDD(lines);

		// input: F-G-0, D-F-1
		// output: <F-G, 0>, <D-F, 1>
		JavaPairRDD<String, Integer> mapToPairRDD = mapToPairRDD(flatMapRDD);

		JavaPairRDD<String, Iterable<Integer>> groupByKeyRDD = mapToPairRDD.groupByKey();

		JavaPairRDD<Integer, String> sortByKeyRDD = sortedRDD(groupByKeyRDD);

		JavaPairRDD<Integer, String> filterRDD = filterRDD(sortByKeyRDD);

		// showResult(filterRDD);

		recommendProcess(filterRDD);

		closeJsc(jsc);

	}

	/**
	 * A-B-0 表示直接关系
	 * B-C-1 表示间接关系
	 */
	private JavaRDD<String> flatMapRDD(JavaRDD<String> lines) {
		JavaRDD<String> flatMapRDD = lines.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			private String compare(String a, String b) {
				if (a.compareTo(b) < 0) {
					return a + Common.MINUS + b + Common.MINUS;
				} else {
					return b + Common.MINUS + a + Common.MINUS;
				}
			}

			@Override
			public Iterable<String> call(String line) throws Exception {
				String[] split = line.split(Common.BLANK);
				List<String> relationshipList = new ArrayList<>();
				if (split != null && split.length > 0) {
					for (int i = 1; i < split.length; i++) {
						String directRelation = compare(split[0], split[i]) + "0";
						// logger.info(directRelation);
						relationshipList.add(directRelation);
						for (int j = i + 1; j < split.length; j++) {
							String indirectRelation = compare(split[i], split[j]) + "1";
							relationshipList.add(indirectRelation);
							// logger.info(indirectRelation);
						}
					}
				}

				// output:
				// [A-B-0, B-C-1, B-G-1, A-C-0, C-G-1, A-G-0]
				// [A-B-0, A-E-1, B-E-0]
				// [A-C-0, A-D-1, A-E-1, C-D-0, D-E-1, C-E-0]
				// [C-D-0, C-E-1, C-G-1, D-E-0, E-G-1, D-G-0]
				// [B-E-0, B-C-1, B-D-1, B-F-1, B-G-1, C-E-0, C-D-1, C-F-1, C-G-1, D-E-0, D-F-1, D-G-1, E-F-0, F-G-1, E-G-0]
				// [E-F-0, E-G-1, F-G-0]
				// [A-G-0, A-D-1, A-E-1, A-F-1, D-G-0, D-E-1, D-F-1, E-G-0, E-F-1, F-G-0]

				// String[] a = new String[relationshipList.size()];
				// relationshipList.toArray(a);
				// logger.info(Arrays.toString(a));
				return relationshipList;
			}
		});
		return flatMapRDD;
	}

	/**
	 * input: F-G-0, D-F-1
	 * output: <F-G, 0>, <D-F, 1>
	 */
	private JavaPairRDD<String, Integer> mapToPairRDD(JavaRDD<String> flatMapRDD) {
		JavaPairRDD<String, Integer> mapToPairRDD = flatMapRDD.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String str) throws Exception {
				String[] split = str.split(Common.MINUS);
				return new Tuple2<String, Integer>(split[0] + Common.MINUS + split[1], Integer.valueOf(split[2]));
			}
		});
		return mapToPairRDD;
	}

	/**
	 * 对间接关系的记录进行统计: <1,A-F>, <2,D-F>
	 * 忽略统计直接关系的记录    : <0,C-E>, <0,C-D>
	 */
	private JavaPairRDD<Integer, String> sortedRDD(JavaPairRDD<String, Iterable<Integer>> groupByKeyRDD) {
		JavaPairRDD<Integer, String> sortByKeyRDD = groupByKeyRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, Integer, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Iterable<Integer>> t) throws Exception {
				Iterator<Integer> iterator = t._2.iterator();

				boolean isDirectRelation = false;
				int sum = 0;
				while (iterator.hasNext()) {
					if (iterator.next() == 0) {
						isDirectRelation = true;
						break;
					}
					sum++;
				}

				if (!isDirectRelation) {
					return new Tuple2<Integer, String>(sum, t._1);
				}
				return new Tuple2<Integer, String>(0, t._1);
			}
		}).sortByKey(false);
		return sortByKeyRDD;
	}

	/**
	 * 把直接关系的记录排除掉: <0,C-E>, <0,C-D>
	 * 那么剩下的就是间接关系的记录： <1,A-F>, <2,D-F>
	 */
	private JavaPairRDD<Integer, String> filterRDD(JavaPairRDD<Integer, String> sortByKeyRDD) {
		JavaPairRDD<Integer, String> filterRDD = sortByKeyRDD.filter(new Function<Tuple2<Integer, String>, Boolean>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<Integer, String> t) throws Exception {
				return t._1 != 0;
			}
		});
		return filterRDD;
	}

	/**
	 * 好友推荐
	 */
	private void recommendProcess(JavaPairRDD<Integer, String> filterRDD) {

		/**
		 * 把间接关系记录： <2,D-F>， 封装到MyRelationShip里面
		 * 一条记录生成两个对象，即：D-F-2,F-D-2
		 */
		JavaRDD<MyRelationShip> flatMapRelationShipRDD = filterRDD.flatMap(new FlatMapFunction<Tuple2<Integer, String>, MyRelationShip>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<MyRelationShip> call(Tuple2<Integer, String> t) throws Exception {
				String key = t._2;
				String[] split = key.split(Common.MINUS);
				List<MyRelationShip> list = new ArrayList<>();
				// 为什么这里要里使用自定义的类来封装呢？
				// 主要是为了之后的排序
				MyRelationShip relationShipAB = new MyRelationShip(split[0], split[1], t._1);
				MyRelationShip relationShipBA = new MyRelationShip(split[1], split[0], t._1);
				list.add(relationShipAB);
				list.add(relationShipBA);
				return list;
			}
		});

		/**
		 * 对D-F-2,F-D-2记录进行再次封装
		 * output: <D, D-F-2>,<F, F-D-2>
		 */
		JavaPairRDD<String, MyRelationShip> mapToPairRelationShipRDD = flatMapRelationShipRDD.mapToPair(new PairFunction<MyRelationShip, String, MyRelationShip>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, MyRelationShip> call(MyRelationShip vo) throws Exception {
				return new Tuple2<String, MyRelationShip>(vo.getSelfName(), vo);
			}
		});

		// 分组和排序
		JavaPairRDD<String, Iterable<MyRelationShip>> groupByKey = mapToPairRelationShipRDD.groupByKey().sortByKey();

		// 结果展示
		showResult(groupByKey);
	}

	/**
	 * output:
	 * A , Recommended : E, weith : 3
	 * A , Recommended : D, weith : 2
	 * B , Recommended : C, weith : 2
	 * B , Recommended : G, weith : 2
	 * C , Recommended : G, weith : 3
	 * C , Recommended : B, weith : 2
	 * D , Recommended : F, weith : 2
	 * D , Recommended : A, weith : 2
	 * E , Recommended : A, weith : 3
	 * F , Recommended : D, weith : 2
	 * F , Recommended : A, weith : 1
	 * G , Recommended : C, weith : 3
	 * G , Recommended : B, weith : 2
	 * 
	 * 
	 * 从输出结果可以看出：
	 * 给A推荐的话，分别是E,D
	 * 给B推荐的话，分别是C,G
	 * 给C推荐的话，分别是G,B
	 * .....
	 * 
	 */
	private void showResult(JavaPairRDD<String, Iterable<MyRelationShip>> groupByKey) {
		groupByKey.foreach(new VoidFunction<Tuple2<String, Iterable<MyRelationShip>>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Iterable<MyRelationShip>> t) throws Exception {
				Iterator<MyRelationShip> iterator = t._2.iterator();
				List<MyRelationShip> list = new ArrayList<>();
				while (iterator.hasNext()) {
					list.add(iterator.next());
				}

				Collections.sort(list);

				if (list != null && list.size() > 0) {
					// 推荐Top n的好友
					int num = Math.min(FriendRecommendationMain.TOP_N, list.size());
					for (int i = 0; i < num; i++) {
						MyRelationShip myRelationShip = list.get(i);
						if (myRelationShip != null) {
							logger.info(t._1 + " , Recommended : " + myRelationShip.getOtherName() + ", weith : " + myRelationShip.getWeight());
						}
					}
				}
			}
		});
	}

	private void closeJsc(JavaSparkContext jsc) {
		jsc.stop();
	}
}

class MyRelationShip implements Comparable<MyRelationShip>, Serializable {

	private static final long serialVersionUID = 1L;

	private String selfName;
	private String otherName;
	private int weight;

	public MyRelationShip(String selfName, String otherName, int weight) {
		super();
		this.selfName = selfName;
		this.otherName = otherName;
		this.weight = weight;
	}

	@Override
	public int compareTo(MyRelationShip o) {
		if (this.selfName.hashCode() < o.selfName.hashCode()) {
			if (this.otherName.hashCode() < o.otherName.hashCode()) {
				return this.weight - o.weight;
			}
			return this.otherName.hashCode() - o.otherName.hashCode();
		}
		return this.selfName.hashCode() - o.selfName.hashCode();
	}

	public String getSelfName() {
		return selfName;
	}

	public void setSelfName(String selfName) {
		this.selfName = selfName;
	}

	public String getOtherName() {
		return otherName;
	}

	public void setOtherName(String otherName) {
		this.otherName = otherName;
	}

	public int getWeight() {
		return weight;
	}

	public void setWeight(int weight) {
		this.weight = weight;
	}

}
