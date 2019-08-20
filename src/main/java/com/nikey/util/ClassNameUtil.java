package com.nikey.util;

import java.util.Map;

/**
 * @author jayzee
 * @date 26 Sep, 2014
 * a utility class to construct class name
 */
public class ClassNameUtil {

	/**
	 * @date 25 Sep, 2014
	 * @param value
	 * @return className
	 *	construct class name
	 */
	public static String constructClassNameForService(Map<String, String[]> value) {
		String[] temp = value.get(PropUtil.getString("WEB_SERVICE_TYPE"));
		if(temp == null || temp.length == 0 || "".equals(temp[0])) return null;
		else {
			String type = temp[0].toLowerCase();
			String first_letter = type.charAt(0) + "";
			type = first_letter.toUpperCase() + type.substring(1, type.length());
			type = PropUtil.getString("web_package_name") + type + PropUtil.getString("web_interface_name");
			return type;
		}
	}
	
	/**
	 * @date 25 Sep, 2014
	 * @param value
	 * @return className
	 *	construct class name
	 */
	public static String constructClassNameForMapper(Map<String, String[]> value) {
		String[] temp = value.get(PropUtil.getString("LIB_CURVE_VALUE_TYPE"));
		if(temp == null || temp.length == 0 || "".equals(temp[0])) return null;
		else {
			String type = temp[0].toLowerCase();
			//如果type为jsonr则存入redis
			if (type.equals("jsonr")) {
				String[] data = value.get(type);
				if (data != null && data.length > 0) {
					type = handleClassName(data);
				}else {
					return null;
				}
			}else {
				String first_letter = type.charAt(0) + "";
				type = first_letter.toUpperCase() + type.substring(1, type.length());
				type = PropUtil.getString("package_name") + type + PropUtil.getString("interface_name")+"_json";
			}
			
			return type;
		}
	}
	
	private static String handleClassName(String[] data) {
		String type = null;
		Map<String, Object> dataMap = JsonUtil.fromJsonToHashMap(data[0]);
		String dataType = dataMap.get("data_type").toString();
		String[] dataTypes = dataType.split("_");
		if (dataTypes.length > 0) {
			String className = "";
			for(int i = 0; i < dataTypes.length; i ++) {
				if (i == 0) {
					String first_letter = dataTypes[i].charAt(0) + "";
					className = className + first_letter.toUpperCase() + dataTypes[i].substring(1, dataTypes[i].length());
				}else {
					className = className + dataTypes[i];
				}
			}
			type = PropUtil.getString("redis_package_name") + className + PropUtil.getString("redis_interface_name")+"_jsonr";
		}
		return type;
	}
	
	/**
	 * @date 25 Sep, 2014
	 * @param value
	 * @return className
	 *	construct class name
	 */
	public static String constructClassNameForMapper(String value) {
		String type = value.toLowerCase();
		String first_letter = type.charAt(0) + "";
		type = first_letter.toUpperCase() + type.substring(1, type.length());
		type = PropUtil.getString("package_name") + type + PropUtil.getString("interface_name");
		return type;
	}
	
}
