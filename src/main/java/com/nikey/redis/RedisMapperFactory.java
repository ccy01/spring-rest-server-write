package com.nikey.redis;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RedisMapperFactory {
	/**
	 * singleton
	 */
	private static final RedisMapperFactory instance = new RedisMapperFactory();
	private RedisMapperFactory() {
		
	}
	public static RedisMapperFactory instance() {
		return instance;
	}
	
	private final Map<String, RedisMapper> redisMapperMap = new ConcurrentHashMap<String, RedisMapper>();
	
	public RedisMapper getRedisMapper(String className) {
		RedisMapper redisMapper = redisMapperMap.get(className);
		if (redisMapper == null) {
			try {
				/**
				 * check class.forName
				 */
				redisMapper = (RedisMapper) Class.forName(className).newInstance();
				/**
				 * cache into the map
				 */
				redisMapperMap.put(className, redisMapper);
			} catch (Exception e) {
				redisMapper = null;
			}
		}
		return redisMapper;
	}
}
