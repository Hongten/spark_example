/**
 * Spark Example  
 * Mail: hongtenzone@foxmail.com 
 * Blog: http://www.cnblogs.com/hongten
 */
package com.hongten.spark.example.traffic.datagenerate.util;

import com.hongten.spark.example.traffic.datagenerate.Common;

/**
 * @author Hongten
 * @created 20 Jan, 2019
 */
public class StringUtils {

	public static String fulfuill(String str) {
		if (str.length() == 1) {
			return Common.ZERO + str;
		}
		return str;
	}
}
