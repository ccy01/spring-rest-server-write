package com.nikey.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.nikey.bean.AlarmReceiver;
import com.nikey.bean.PowerDemand;
import com.nikey.mapper.AlarmMapper;
import com.nikey.mapper.KeyValueMapper;
import com.nikey.mapper.TbPowerMapper;
import com.nikey.util.DateUtil;
import com.nikey.util.ServiceHelper;

/**
 * 容转需服务的业务类
 * 
 * @author JayzeeZhang
 * @date 2018年3月27日
 */
@Service
public class HokoCapacityToDemandService {
    
    @Autowired
    private TbPowerMapper tbPowerMapper;
    
    @Autowired
    private KeyValueMapper keyValueMapper;
    
    @Autowired
    private AlarmMapper alarmMapper;
    
    public HokoCapacityToDemandService() {
        ServiceHelper.instance().setHokoCapacityToDemandService(this);
    }
    
    
    
    /**
     * 获取电源合同需量信息
     */
    public List<PowerDemand> getPowerDemands() {
        String start = DateUtil.getBeginDayOfThisMonth();
        String end = DateUtil.getBeginDayOfNextMonth();
        return tbPowerMapper.getPowerDemands(start, end);
    }
    
    /**
     * 获取电源合同需量信息
     */
    public PowerDemand getPowerDemands(Integer powerId) {
        String start = DateUtil.getBeginDayOfThisMonth();
        String end = DateUtil.getBeginDayOfNextMonth();
        return tbPowerMapper.getPowerDemand(powerId, start, end);
    }

    public KeyValueMapper getKeyValueMapper() {
        return keyValueMapper;
    }

    public AlarmMapper getAlarmMapper() {
        return alarmMapper;
    }

    public TbPowerMapper getTbPowerMapper() {
        return tbPowerMapper;
    }
}
