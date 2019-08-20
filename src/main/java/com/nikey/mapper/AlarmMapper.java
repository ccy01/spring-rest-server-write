package com.nikey.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import com.nikey.bean.AlarmCounter;
import com.nikey.bean.AlarmReceiver;
import com.nikey.bean.AlarmsetThreshold;
import com.nikey.bean.RealtimeAlarm;

/**
 * 阈值、预警、报警、计数相关
 * 
 * @author JayzeeZhang
 * @date 2018年3月29日
 */
public interface AlarmMapper {
	
	/**
	 * 获取收件人号码
	 * 
	 * @return
	 */
	List<AlarmReceiver> getPhoneNumber();
	
    /**
     * 获取预警类型
     * 
     * @return
     */
    List<AlarmsetThreshold> getAlarmTypeNames();
    
    /**
     * 获取所有电源的最大需量阈值
     * 
     * @return
     */
    List<AlarmsetThreshold> getAlarmsetThresholds();
    
    /**
     * 获取所有电源的预警、报警的日、月计数器
     */
    List<AlarmCounter> getAlarmCounters();
    
    /**
     * 写入预警报警计数器（SAVE OR UPDATE）
     * 
     * @param alarmCounter
     */
    void insertAlarmCounter(AlarmCounter alarmCounter);
    
    /**
     * 写入预警报警（SAVE OR UPDATE）
     * 
     * @param realtimeAlarm
     */
    void insertRealtimeAlarm(RealtimeAlarm realtimeAlarm);
    
    /**
     * 重置最大需量预警、报警日计数器
     */
    void clearDayCounter();
    void clearSpecificDayCounter(@Param("device_id") Integer powerId, @Param("customer_id") Integer companyId); // device_id => 电源ID
    
    /**
     * 重置最大需量预警、报警月计数器
     */
    void clearMonthCounter();
    void clearSpecificMonthCounter(@Param("device_id") Integer powerId, @Param("customer_id") Integer companyId); // device_id => 电源ID
}
