package com.nikey.mapper;

import java.util.List;
import java.util.Map;

import org.apache.ibatis.annotations.Param;

import com.nikey.bean.PostData;

public interface MonitordataMapper
{
    
    Map<String,Object> getMonitorDataById(short id);

	/**
	 * 将post到hbase的数据，暂存到MySQL，待远端Redis恢复后再写回
	 * 
	 * @author JayzeeZhang
	 * @param json
	 */
	void writeHbaseData(@Param("json") String json);
	
	/**
	 * 取出暂存在MySQL的post data
	 * 
	 * @param id
	 * @param num
	 * @return
	 */
	List<PostData> getHbaseData(@Param("id") Long id, @Param("num") int num);
}
