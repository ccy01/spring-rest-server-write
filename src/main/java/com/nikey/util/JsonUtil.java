package com.nikey.util;

import java.lang.reflect.Type;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

/**
 * @author jayzeee
 * @date 4:15:40 PM
 * @description JSON UTIL
 */
public class JsonUtil {
    
    private JsonUtil(){}
    
	static class Singleton {
		private Gson gson = null;
		Singleton() {
			gson = new Gson();
		}
		public Gson getGson() {
			return gson;
		}
	}
	
	/**
	 * 目前使用的Gson解析json数据
	 * @return
	 */
	private static Gson getGson() {
		return new Singleton().getGson();
	}
	
	/**
	 * 将对象转换为json数据
	 * @param src
	 * @return
	 */
	public static String toJson(Object src) {
		return getGson().toJson(src);
	}
	
	/**
	 * 解析json数据, 转换为clazz对象
	 * @param json
	 * @param clazz
	 * @return
	 */
	public static Object fromJson(String json, Class<?> clazz) {
		return getGson().fromJson(json, clazz);
	}
	
	/**
	 * Handy tool to create json file with specified date format
	 * @param src
	 * @return
	 */
	public static String toLongDateFormatJson (Object src) {
	    Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss")
                .create();
        return gson.toJson(src);
	}
	public static String toLongDate_FormatJson (Object src) {
	    Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd_HH:mm:ss")
                .create();
        return gson.toJson(src);
	}
	
	/**
	 * @author jayzeee
	 * @date 4:17:27 PM
	 * @description getGenericList
	 * @param jsonString
	 * @param cls
	 * @return
	 */
	public static Object fromJsonAsGeneric(String jsonString, Type typeOfT) {
		return getGson().fromJson(jsonString, typeOfT);
    }
	
	/**
	 * @date 30 Sep, 2014
	 * @param json
	 * @return Map<String, Object>
	 *	from json to hashmap
	 */
	public static Map<String, Object> fromJsonToHashMap(String json) {
		Type stringStringMap = new TypeToken<Map<String, Object>>(){}.getType();
		return getGson().fromJson(json, stringStringMap);  //fromJson()将指定的json反序列化为指定类型的对象。
	}
	
	public static Map<String, String[]> fromJsonToStringHashMap(String json) {
		Type stringStringMap = new TypeToken<Map<String, String[]>>(){}.getType();
		return getGson().fromJson(json, stringStringMap);
	}
}
