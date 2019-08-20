package com.nikey.redis;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nikey.util.JsonUtil;
import com.nikey.util.RedisUtil;

import redis.clients.jedis.Jedis;

public class WatermotortempRedisMapper implements RedisMapper {

	/**
	 * slfj
	 */
	Logger logger = LoggerFactory.getLogger(getClass());
	
	private Jedis jedis = RedisUtil.getInstance();
	
	@Override
	public void convertParameterMapToRedis(Map<String, String[]> request) {

		String[] data = request.get("jsonr");
		
		Map<String, Object> jsonMap = null;
	    if (data != null && data.length > 0) {
	        try {
	            jsonMap = JsonUtil.fromJsonToHashMap(data[0]);
            } catch (Exception e) {
                logger.info("parse json error, the json string is : " + data[0]);
                e.printStackTrace();
            }
	    }
	    if (jsonMap == null || jsonMap.get("data_type") == null) {
	    	logger.info("the json data is null or is empty : " + data[0]);
	    }
	    
	    int type = Double.valueOf(jsonMap.get("type").toString()).intValue();
	    int device = Double.valueOf(jsonMap.get("device_id").toString()).intValue();
	    int companyId = device / 1000;
	    int deviceId = device % 100;
	    
	    //type为1则是实时曲线，0为刷新数据
	    if (type == 1) {
			RedisUtil.setKey(companyId + "/motortemp/" + deviceId, jedis, data[0]);
		} else if(type == 0) {
			RedisUtil.setKeyData(companyId + "/motortempdata/" + deviceId, jedis, data[0]);
		}
		
	}

}
