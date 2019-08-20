package com.nikey.util;

import java.math.BigDecimal;
import java.util.Date;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * @author ouyang
 * @date 2014-10-22
 * @description 数据格式化
 *
 */
public class DataFormatUtil {
	static Logger log = LoggerFactory.getLogger(DataFormatUtil.class);
	/**
	 * 返回数据四舍五入，带两位小数点
	 * @param data
	 * @return float
	 * 
	 */
	public static float formatFloatTwoBits(float data) {
		// return (float)(Math.round(data*1000)/1000);
		BigDecimal d = new BigDecimal(data);
		float value = d.setScale(2, BigDecimal.ROUND_HALF_UP).floatValue();
		return value;
	}

	/**
	 *  返回数据四舍五入，带三位小数点
	 * @param data
	 * @return float 
	 *
	 */
	public static float formatFloatThreeBits(float data) {
		// return (float)(Math.round(data*1000)/1000);
		BigDecimal d = new BigDecimal(data);
		float value = d.setScale(3, BigDecimal.ROUND_HALF_UP).floatValue();
		return value;
	}
	/**
	 * 返回数据四舍五入，带两位小数点
	 * @param data
	 * @return double
	 */
	public static double formatDoubleTwoBits(double data) {
		// return (double)Math.round(data*1000)/1000;
		BigDecimal d = new BigDecimal(data);
		double value = d.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
		return value;

	}

	/**
	 * 将{long，value}转化为{string，value}
	 * @param longValue
	 * @return stringValue
	 */
	public static Object[] transferTimeValue(Object[] longValue)
	{
		Object[] stringValue = null;
		try {
			 stringValue = new Object[] {
					DateUtil.formatToHHMMSS(new Date((long) longValue[0])),
					longValue[1] };
		} catch (Exception e) {
			log.error("cast longvalue to stringvalue erro");
			return null;
		}
		return stringValue;
	}

	
	
}
