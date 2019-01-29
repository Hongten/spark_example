/**
 * Spark Example  
 * Mail: hongtenzone@foxmail.com 
 * Blog: http://www.cnblogs.com/hongten
 */
package com.hongten.spark.example.traffic.datagenerate.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Serializable;

import org.apache.log4j.Logger;

import com.hongten.spark.example.traffic.datagenerate.Common;

/**
 * @author Hongten
 * @created 20 Jan, 2019
 */
public class FileUtils implements Serializable {

	static final Logger logger = Logger.getLogger(FileUtils.class);
	
	private static final long serialVersionUID = 1L;

	public static boolean createFile(String pathFileName) {
		try {
			File file = new File(pathFileName);
			if (file.exists()) {
				logger.info("Find file" + pathFileName + ", system will delete it now!!!");
				file.delete();
			}
			boolean createNewFile = file.createNewFile();
			logger.info("create file " + pathFileName + " success!");
			return createNewFile;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	public static void saveFile(String pathFileName, String newContent) {
		FileOutputStream fos = null;
		OutputStreamWriter osw = null;
		PrintWriter pw = null;
		try {
			String content = newContent;
			File file = new File(pathFileName);
			fos = new FileOutputStream(file, false);
			osw = new OutputStreamWriter(fos, Common.CHARSETNAME_UTF_8);
			pw = new PrintWriter(osw);
			pw.write(content);
			pw.close();
			osw.close();
			fos.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (pw != null) {
				pw.close();
			}
			if (osw != null) {
				try {
					osw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (fos != null) {
				try {
					fos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
