package com.hongten.spark.example.traffic.datagenerate.util;

import com.hongten.spark.example.traffic.datagenerate.Common;

public class StringUtils {

	public static String fulfuill(String str) {
		if (str.length() == 1) {
			return Common.ZERO + str;
		}
		return str;
	}
}
