/**
 * Spark Example  
 * Mail: hongtenzone@foxmail.com 
 * Blog: http://www.cnblogs.com/hongten
 */
package com.hongten.spark.example.streaming.util;

import java.util.Map;
import java.util.TreeMap;

import com.hongten.spark.example.traffic.datagenerate.Common;

/**
 * @author Hongten
 * @created 29 Jan, 2019
 */
public class FileUtils {

	static Map<Integer, Integer> result = new TreeMap<Integer, Integer>();

	public static void generateFile(String path, Map<Integer, Integer> resultMap, int refreshTime, String dateTime) throws Exception {
		// 更新数据
		if (resultMap != null) {
			System.out.println("update size : " + resultMap.size());
			for (Map.Entry<Integer, Integer> entry : resultMap.entrySet()) {
				result.put(entry.getKey(), entry.getValue());
			}
		}
		
		com.hongten.spark.example.traffic.datagenerate.util.FileUtils.saveFile(path, getContent(refreshTime, "Real Time Monitor - " + dateTime, result));
	}

	private static String getContent(int microSecond, String tableTitle, Map<Integer, Integer> resultMap) {
		StringBuffer sb = new StringBuffer();

		sb.append("<!DOCTYPE html>").append(Common.LINE_BREAK);
		sb.append("<html>").append(Common.LINE_BREAK);
		sb.append("<head>").append(Common.LINE_BREAK);
		// style
		sb.append("<style>").append(Common.LINE_BREAK);
		sb.append("table, th, td {").append(Common.LINE_BREAK);
		sb.append("border: 1px solid black;").append(Common.LINE_BREAK);
		sb.append("border-collapse: collapse;").append(Common.LINE_BREAK);
		sb.append("}").append(Common.LINE_BREAK);
		sb.append("th, td {").append(Common.LINE_BREAK);
		sb.append("padding: 5px;").append(Common.LINE_BREAK);
		sb.append("text-align: left;").append(Common.LINE_BREAK);
		sb.append("}").append(Common.LINE_BREAK);
		sb.append("</style>").append(Common.LINE_BREAK);
		// javascript
		sb.append("<script type=\"text/JavaScript\">").append(Common.LINE_BREAK);
		sb.append("<!--").append(Common.LINE_BREAK);
		sb.append("function autoRefresh( t ) {").append(Common.LINE_BREAK);
		sb.append("setTimeout(\"location.reload(true);\", t);").append(Common.LINE_BREAK);
		sb.append("}").append(Common.LINE_BREAK);
		sb.append("//  -->").append(Common.LINE_BREAK);
		sb.append("</script>").append(Common.LINE_BREAK);
		sb.append("</head>").append(Common.LINE_BREAK);
		sb.append("<body onload=\"JavaScript:autoRefresh(" + (microSecond * 1000) + ");\">").append(Common.LINE_BREAK);

		sb.append("<table style=\"width:100%\">").append(Common.LINE_BREAK);
		sb.append("<caption>" + tableTitle + "</caption>").append(Common.LINE_BREAK);

		int index = 6;
		for (int i = 1; i <= Common.ROAD_NUM; i++) {
			int roadId = 10000 + i;// 10001

			int monitorB = roadId * 2;// 20002
			int monitorA = monitorB - 1;// 20001

			int monitorAValue = resultMap.get(monitorA) == null ? 0 : resultMap.get(monitorA);
			int monitorBValue = resultMap.get(monitorB) == null ? 0 : resultMap.get(monitorB);

			if (i == 1) {
				sb.append("<tr>").append(Common.LINE_BREAK);
			}

			if (i == index) {
				index += 5;
				sb.append("</tr>").append(Common.LINE_BREAK);
				sb.append("<tr>").append(Common.LINE_BREAK);
			}

			sb.append("<th " + getColor(monitorAValue) + ">" + monitorA + " : " + monitorAValue + "</th>").append("<th " + getColor(monitorBValue) + ">" + monitorB + " : " + monitorBValue + "</th>");
		}
		sb.append("</table>").append(Common.LINE_BREAK);

		sb.append("</br>").append(Common.LINE_BREAK);
		sb.append("</br>").append(Common.LINE_BREAK);
		sb.append("<table align=\"left\" style=\"width:20%\">").append(Common.LINE_BREAK);
		sb.append("<tr>").append(Common.LINE_BREAK);
		sb.append("<th " + getColor(2) + ">直接堵上了</th>");
		sb.append("</tr>").append(Common.LINE_BREAK);

		sb.append("<tr>").append(Common.LINE_BREAK);
		sb.append("<th " + getColor(8) + ">严重拥堵，但是还可以移动</th>");
		sb.append("</tr>").append(Common.LINE_BREAK);

		sb.append("<tr>").append(Common.LINE_BREAK);
		sb.append("<th " + getColor(25) + ">行驶缓慢</th>");
		sb.append("</tr>").append(Common.LINE_BREAK);

		sb.append("<tr>").append(Common.LINE_BREAK);
		sb.append("<th " + getColor(40) + ">正常通行</th>");
		sb.append("</tr>").append(Common.LINE_BREAK);
		sb.append("</table>").append(Common.LINE_BREAK);

		sb.append("</body>").append(Common.LINE_BREAK);
		sb.append("</html>").append(Common.LINE_BREAK);
		return sb.toString();
	}

	public static String getColor(int averageVehicleSpeed) {
		String colorStr = "";
		if (averageVehicleSpeed <= 5) {
			colorStr = "hsla(9, 100%, 64%, 1)";
		} else if (averageVehicleSpeed > 5 && averageVehicleSpeed <= 10) {
			colorStr = "hsla(9, 100%, 64%, 0.6)";
		} else if (averageVehicleSpeed > 10 && averageVehicleSpeed <= 30) {
			colorStr = "hsla(9, 100%, 64%, 0.2)";
		} else {
			colorStr = "hsl(147, 50%, 47%)";
		}
		return "style=\"background-color:" + colorStr + "\"";
	}
}
