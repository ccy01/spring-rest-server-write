package com.nikey.mapper;

import java.util.List;
import java.util.Map;

import org.apache.ibatis.annotations.Param;

import com.nikey.bean.HokoPointinfo;
import com.nikey.bean.PowerDemand;
import com.nikey.bean.PowerInfo;

/**
 * 电源信息 & 电量信息
 * 
 * @author JayzeeZhang
 * @date 2018年3月27日
 */
public interface TbPowerMapper {
	
	/**
	 * 获取pointinfo
	 * 
	 * @return
	 */
	List<HokoPointinfo> getHokoPointinfos();
	
	/**
	 * 获取单个pointinfo
	 * 
	 * @param companyId
	 * @param deviceId
	 * @return
	 */
	HokoPointinfo getHokoPointinfo(@Param("companyId") Integer companyId, @Param("deviceId") Integer deviceId);
	
	/**
	 * 获取电源信息
	 * 
	 * @return
	 */
	List<PowerInfo> getPowerInfos();
	
	/**
	 * 获取电源合同需量信息
	 * 
	 * @param start
	 * @param end
	 * @return
	 */
	List<PowerDemand> getPowerDemands(@Param("start") String start, @Param("end") String end);
	
	/**
	 * 获取指定电源的需量
	 * 
	 * @param powerId
	 * @param start
	 * @param end
	 * @return
	 */
	PowerDemand getPowerDemand(@Param("powerId") Integer powerId, @Param("start") String start, 
			@Param("end") String end);

}
