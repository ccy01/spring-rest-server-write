package com.nikey.fix;

import com.nikey.util.DateUtil;

/**
 * @author Jayzee
 * @date 2016年7月11日 下午10:18:11
 * 
 * 构建道格拉斯压缩返算的命令
 */
public class ConstructCommand {

	String hour = "java -jar /home/gx/job/douglas.jar hour ";
	String StartTime = "2016-06-17 00:00:00";
	String EndTime = "2016-07-12 00:00:00";

	public static void main(String[] args) {
		ConstructCommand tt = new ConstructCommand();
		tt.tt();
	}

	private void tt() {
		long start = DateUtil.parseHHMMSSToDate(StartTime).getTime();
		long end = DateUtil.parseHHMMSSToDate(EndTime).getTime();
		while (start < end) {
			System.out.println(String.format("%s '%s' '%s'", hour,
					DateUtil.formatToHHMMSS(start),
					DateUtil.formatToHHMMSS(start += (24 * 3600 * 1000l))));
		}
	}

}
