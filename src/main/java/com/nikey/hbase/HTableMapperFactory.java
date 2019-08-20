package com.nikey.hbase;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author jayzee
 * @date 25 Sep, 2014
 *	factory to get HTableMapper
 */
public class HTableMapperFactory {
	
	/**
	 * singleton
	 */
	private static final HTableMapperFactory instance = new HTableMapperFactory();
	private HTableMapperFactory() {}
	public static HTableMapperFactory instance() {
		return instance;
	}
	
	/**
	 * map container to store the existed HTableMapper
	 */
	private final Map<String, HTableMapper> hTableMapperMap = new ConcurrentHashMap<String, HTableMapper>();
	
	/**
	 * @date 25 Sep, 2014
	 * @param className
	 * @return HTableMapper or null
	 *	get the HTableMapper instance by className
	 */
	public HTableMapper getHTableMapper(String className) {
		HTableMapper hTableMapper = hTableMapperMap.get(className);
		if(hTableMapper == null) {
			try {
				/**
				 * check class.forName
				 */
				hTableMapper = (HTableMapper) Class.forName(className).newInstance();
				/**
				 * cache into the map
				 */
				hTableMapperMap.put(className, hTableMapper);
			} catch (Exception e) {
				hTableMapper = null;
			}
		}
		return hTableMapper;
	}
	
}
