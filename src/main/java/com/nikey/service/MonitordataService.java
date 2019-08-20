package com.nikey.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.nikey.bean.PostData;
import com.nikey.mapper.MonitordataMapper;
import com.nikey.util.ServiceHelper;

@Service
public class MonitordataService {

	/**
	 * slfj
	 */
	Logger logger = LoggerFactory.getLogger(getClass());

	public MonitordataService() {
		ServiceHelper.instance().setMonitordataService(this);
	}

	@Autowired
	private MonitordataMapper monitordataMapper;

	private Map<Short, Map<String, Object>> cache = new HashMap<Short, Map<String, Object>>();

	public Map<String, Object> getRelationMap(Short id) {
		synchronized (cache) {
			if (cache.get(id) != null) {
				return cache.get(id);
			}
		}
		try {
			Map<String, Object> result = new HashMap<String, Object>();
			result = monitordataMapper.getMonitorDataById(id);
			if (result != null) {
				synchronized (cache) {
					cache.put(id, result);
				}
			}
			return result;
		} catch (Exception e) {
			logger.error("Real temperatrue device {} can't get relation in MySQL !", id);
			return null;
		}
	}

	/**
	 * 将写入到remote redis失败的数据，暂时缓存到mysql
	 * 
	 * @param json
	 *            写入到mysql的hbase的post数据
	 */
	public synchronized void writeHbaseData(String json) {
		try {
			monitordataMapper.writeHbaseData(json);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("cache hbase data to MySQL error : " + json);
		}
	}
	
	/**
	 * 取出暂存在MySQL的post data
	 * 
	 * @param id
	 * @param num
	 * @return
	 */
	public List<PostData> getHbaseData(Long id, int num) {
		return monitordataMapper.getHbaseData(id, num);
	}

}
