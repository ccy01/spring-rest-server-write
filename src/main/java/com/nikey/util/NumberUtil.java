package com.nikey.util;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jayzee
 *	number util
 */
public class NumberUtil {
	
	/**
	 * slfj
	 */
	static Logger logger = LoggerFactory.getLogger(NumberUtil.class);
    
    /**
     * 返回true表示数值为0
     * @param value
     * @return boolean
     */
	public static boolean isZero(Float value) {
		if (Math.abs(value) > 0.001)
			return false;
		return true;
    }
	
	public static short Short_valueOf(Map<String, String[]> request, String param) {
		short value = Short.valueOf(request.get(param)[0]);
		
		String msg = "The " + param + " is error in the request-map, it's value is " + value;
		
		if(value < 0) {
			logger.error(PropUtil.getString("ERR006"), msg);
			throw new RuntimeException(msg);
		}
		return value;		
	}

	public static int Int_valueOf(Map<String, String[]> request, String param) {
		int value = Integer.valueOf(request.get(param)[0]);

		String msg = "The " + param + " is error in the request-map, it's value is " + value;

		if(value < 0) {
			logger.error(PropUtil.getString("ERR006"), msg);
			throw new RuntimeException(msg);
		}
		return value;
	}
	
	public static long Long_valueOf(Map<String, String[]> request, String param) {
		long value = Long.valueOf(request.get(param)[0]);
		
		String msg = "The " + param + " is error in the request-map, it's value is " + value;
		
		if(value < 0) {
			logger.error(PropUtil.getString("ERR006"), msg);
			throw new RuntimeException(msg);
		}
		return value;		
	}
	
	/**
	 * 获取request的对象key的值，如果有对应"Ua","Ub,"Uc"就会报异常
	 * 
	 */
	public static float Float_valueOf(Map<String, String[]> request, String param) {
		float value = Float.valueOf(request.get(param)[0]);
		String msg = "The " + param + " is error in the request-map, it's value is " + value;
		
		if(("Ua".equals(param)
				|| "Ub".equals(param)
						|| "Uc".equals(param)) && value <0 ) {
			logger.error(PropUtil.getString("ERR006"), msg);
			throw new RuntimeException(msg);
		}
		return value;		
	}
	
	public static double Double_valueOf(Map<String, String[]> request, String param) {
		double value = Double.valueOf(request.get(param)[0]);
		return value;		
	}

	public static Long longValue(Object time) {
		return Double.valueOf(time.toString()).longValue();
	}

	public static int intValue(Object time) {
		return Double.valueOf(time.toString()).intValue();
	}

	public static double doubleValue(Object value) {
		return Double.valueOf(value.toString());
	}

}
