package com.nikey.mapper;

import java.util.List;
import java.util.Map;

import org.apache.ibatis.annotations.Param;

import com.nikey.bean.Abnormaldata;

public interface AbnormalDataMapper {
    
    //缓存事件id和事件名
    Map<String, Object> getEventDataById(int id);
	
    // 查询历史表是否存在该key的数据（防止结束的数据先到）
    int selectAlarmdataHistoryCount(Abnormaldata abnormaldata);
    // 查询温度报警记录历史表是否存在该key的数据（防止结束的数据先到）
    int selectTemperatreAlarmdataHistoryCount(Abnormaldata abnormaldata);
    // 插入实时越限数据
    int insertAlarmdataReal(Abnormaldata abnormaldata);
    // 插入实时温度越限数据
    int insertTemperatureAlarmdataReal(Abnormaldata abnormaldata);
    // 删除越限实时数据  -- 考虑一些奇异值没有来得及删除 --
    int deleteAlarmdataReal(Abnormaldata abnormaldata);
    // 删除温度越限实时数据  -- 考虑一些奇异值没有来得及删除 --
    int deleteTemperatureAlarmdataReal(Abnormaldata abnormaldata);
    // 删除越限历史数据  -- 考虑一些奇异值没有来得及删除 --
    int deleteAlarmdataHistory(Abnormaldata abnormaldata);
    //删除温度越限历史数据  -- 考虑一些奇异值没有来得及删除 --
    int deleteTemperatureAlarmdataHistory(Abnormaldata abnormaldata);
    // 新增越限历史数据
    int insertAlarmdataHistory(Abnormaldata abnormaldata);
    // 新增温度越限历史数据
    int insertTemperatureAlarmdataHistory(Abnormaldata abnormaldata);
    
    // 查询历史表是否存在该key的数据（防止结束的数据先到）
    int selectCommerrHistoryCount(Abnormaldata abnormaldata);
    // 插入实时通信异常数据
    int insertCommerrReal(Abnormaldata abnormaldata);
    // 删除通信异常实时数据  -- 考虑一些奇异值没有来得及删除 --
    int deleteCommerrReal(Abnormaldata abnormaldata);
    // 删除通信异常历史数据  -- 考虑一些奇异值没有来得及删除 --
    int deleteCommerrHistory(Abnormaldata abnormaldata);
    // 新增通信异常历史数据
    int insertCommerrHistory(Abnormaldata abnormaldata);
    //插入每工艺用电量
//    int insertCraftEpi(int deviceId, long changeTime, double epi, int operationType, int isGroup);
    int insertCraftEpi(@Param("deviceId") int deviceId, @Param("changeTime") String changeTime, 
    		@Param("epi") double epi, @Param("operationType") int operationType, @Param("isGroup") int isGroup);
    
    List<Map<String, Object>> selectAlarmdata();
	
}
