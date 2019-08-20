package com.nikey.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author jayzee
 * @date 26 Sep, 2014
 *	A factory to produce GetDataFromHbaseService
 */
public class GetDataFromHbaseServiceFactory {
	
	/**
	 * singleton
	 */
	private static final GetDataFromHbaseServiceFactory instance = new GetDataFromHbaseServiceFactory();
	private GetDataFromHbaseServiceFactory() {}
	public static GetDataFromHbaseServiceFactory instance() {
		return instance;
	}
	
	/**
	 * map container to store the existed GetDataFromHbaseService
	 */
	private final Map<String, GetDataFromHbaseService> getDataFromHbaseServiceMap = new ConcurrentHashMap<String, GetDataFromHbaseService>();

	/**
	 * @date 25 Sep, 2014
	 * @param className
	 * @return HTableMapper or null
	 *	get the GetDataFromHbaseService instance by className
	 */
	public GetDataFromHbaseService getGetDataFromHbaseService(String className) {
		GetDataFromHbaseService service = getDataFromHbaseServiceMap.get(className);
		if(service == null) {
			try {
				/**
				 * check class.forName
				 */
				service = (GetDataFromHbaseService) Class.forName(className).newInstance();
				/**
				 * cache into the map
				 */
				getDataFromHbaseServiceMap.put(className, service);
			} catch (Exception e) {
				service = null;
			}
		}
		return service;
	}
	
}
