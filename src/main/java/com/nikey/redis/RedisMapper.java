package com.nikey.redis;

import java.util.Map;

public interface RedisMapper {
	
	/**
	 * @date 25 Sep, 2014
	 * @param value
	 * @return Put
	 *	convert the parameter map to put
	 */
	void convertParameterMapToRedis(Map<String, String[]> request);
	
}
