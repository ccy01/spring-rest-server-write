package com.nikey.mapper;

import org.apache.ibatis.annotations.Param;

/**
 * tb_key_value
 * 
 * @author JayzeeZhang
 * @date 2018年3月28日
 */
public interface KeyValueMapper {
	
	String get(@Param("key") String key);
	
	void put(@Param("key") String key, @Param("value") String value);
	
	void del(@Param("key") String key);

}
