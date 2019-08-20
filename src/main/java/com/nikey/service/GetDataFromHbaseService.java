package com.nikey.service;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;

/**
 * @author jayzee
 * @date 26 Sep, 2014
 *	Get Data From Hbase Service Interface
 */
public interface GetDataFromHbaseService {
	
	/**
	 * @date 26 Sep, 2014
	 * @param request
	 * @return	Map<String, Object>
	 *	get data from hbase,
	 *	and then construct the value to map
	 */
	Map<String, Object> getDataFromHbase(HttpServletRequest request);

}
