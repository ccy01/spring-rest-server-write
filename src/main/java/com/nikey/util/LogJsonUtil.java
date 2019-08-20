package com.nikey.util;

import java.io.File;
import java.io.FileWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogJsonUtil {
	
	private static Logger logger = LoggerFactory.getLogger(LogJsonUtil.class);

	public static void errorJsonFileRecord(String type, String msg, String json) {
		logger.error("[type]:{} [msg]:{} [json]:{}", new String[]{type, msg, json});
		/*if(json != null) {
			File directory = new File("");// 设定为当前文件夹
			try {
				String folder = directory.getAbsolutePath();
				String path = folder + File.separator + "errorJson"
						+ File.separator + type + File.separator
						+ DateUtil.getThisMoment() + ".txt";

				File file = new File(path);
				if (!file.getParentFile().exists()) {
					file.getParentFile().mkdirs();
				}
				if (!file.exists()) {
					file.createNewFile();
				}
				if (file.exists()) {
					FileWriter writer = new FileWriter(file);
					writer.write("ERROR: " + msg + "\n");
					writer.write(json + "\n");
					writer.flush();
					writer.close();

				}
			} catch (Exception e) {
			}
		}*/
	}

}
