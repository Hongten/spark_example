/**
 * Big Data Example  
 * Mail: hontenzone@foxmail.com 
 * Blog: http://www.cnblogs.com/hongten
 */
package com.hongten.spark.example.traffic.datagenerate;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * From wiki:
 * https://en.wikipedia.org/wiki/Vehicle_registration_plates_of_Singapore<br>
 * 
 * A typical vehicle registration number comes in the format "SKV 6201 B":<br>
 * 
 * <li>S – Vehicle class ("S", with some exceptions, stands for a private
 * vehicle since 1984)</li><br>
 * <li>KV – Alphabetical series ("I" and "O" are not used to avoid confusion
 * with "1" and "0")</li><br>
 * <li>6201 – Numerical series</li><br>
 * <li>B – Checksum letter ("F","I", "N", "O", "Q", "V" and "W" are never used
 * as checksum letters; absent on special government vehicle plates and events
 * vehicle plates)</li>
 * 
 * @author Hongten
 * @created 20 Jan, 2019
 */
public class VehiclePlateGenerateSG implements Serializable {

	private static final long serialVersionUID = -8006144823705880339L;

	public static Random random = new Random();

	// 主要目的就是使得产生的数字字符不在这个list里面
	// 如果在这个list里面找到，那么需要重新产生
	private static List<String> uniqueList = new ArrayList<String>();

	public static String generatePlate() {
		// System.out.println(ALPHABETICAL_ARRAY[18]);// S
		String alphabeticalSeries = getAlphabeticalStr(Common.ALPHABETICAL_ARRAY, random) + getAlphabeticalStr(Common.ALPHABETICAL_ARRAY, random);
		// System.out.println(alphabeticalSeries);//KV

		String numbericalSeries = getNumericalSeriesStr(random);
		// System.out.println(numbericalSeries);//62010

		String checksumLetter = getChecksumLetterStr(Common.ALPHABETICAL_ARRAY, random);
		// System.out.println(checksumLetter);//B

		String singaporeVehiclePlate = Common.ALPHABETICAL_ARRAY[18] + alphabeticalSeries + numbericalSeries + checksumLetter;
		return singaporeVehiclePlate;
	}

	private static String getAlphabeticalStr(String[] ALPHABETICAL_ARRAY, Random random) {
		String alphabeticalStr = Common.ALPHABETICAL_ARRAY[random.nextInt(Common.ALPHABETICAL_ARRAY.length)];
		// "I", "O"
		if (!(alphabeticalStr.equals(Common.ALPHABETICAL_ARRAY[8]) || alphabeticalStr.equals(Common.ALPHABETICAL_ARRAY[14]))) {
			return alphabeticalStr;
		} else {
			return getAlphabeticalStr(Common.ALPHABETICAL_ARRAY, random);
		}
	}

	private static String getNumericalSeriesStr(Random random) {
		// 为了区别真实的车牌，我们把数字设置为5位
		String numericalStr = random.nextInt(10) + "" + random.nextInt(10) + "" + random.nextInt(10) + "" + random.nextInt(10) + "" + random.nextInt(10);
		if (uniqueList.contains(numericalStr)) {
			// 如果存在，则重新产生
			return getNumericalSeriesStr(random);
		} else {
			uniqueList.add(numericalStr);
			return numericalStr;
		}
	}

	private static String getChecksumLetterStr(String[] ALPHABETICAL_ARRAY, Random random) {
		String checksumLetter = ALPHABETICAL_ARRAY[random.nextInt(ALPHABETICAL_ARRAY.length)];
		// "F","I", "N", "O", "Q", "V" and "W"
		if (!(checksumLetter.equals(Common.ALPHABETICAL_ARRAY[5]) || checksumLetter.equals(Common.ALPHABETICAL_ARRAY[8]) || checksumLetter.equals(Common.ALPHABETICAL_ARRAY[13]) || checksumLetter.equals(Common.ALPHABETICAL_ARRAY[14]) || checksumLetter.equals(Common.ALPHABETICAL_ARRAY[16]) || checksumLetter.equals(Common.ALPHABETICAL_ARRAY[21]) || checksumLetter.equals(Common.ALPHABETICAL_ARRAY[22]))) {
			return checksumLetter;
		} else {
			return getChecksumLetterStr(ALPHABETICAL_ARRAY, random);
		}
	}
}
