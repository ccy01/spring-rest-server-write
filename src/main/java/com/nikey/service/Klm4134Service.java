package com.nikey.service;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.nikey.mapper.Klm4134Mapper;
import com.nikey.util.ServiceHelper;

@Service
public class Klm4134Service {
	
	/**
	 * slfj
	 */
	Logger logger = LoggerFactory.getLogger(getClass());
	
	public Klm4134Service() {
		ServiceHelper.instance().setKlm4134Service(this);
	}
	
	@Autowired
	private Klm4134Mapper klm4134Mapper;
	
	private Map<Short, Integer> cache = new HashMap<Short, Integer>();
	
	public int getRelationMap(Short id) {
		synchronized (cache) {
			if(cache.get(id) != null) {
				return cache.get(id);
			}
		}
		try {
			int type = klm4134Mapper.getTemperatureTypeById(id);
			if(type != 0) {
				synchronized (cache) {
					cache.put(id, type);
				}
			}
			return type;
		} catch (Exception e) {
			logger.error("Real temperatrue device {} can't get relation in MySQL !", id);
			return 0;
		}
	}
	

}