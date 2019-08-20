package com.nikey.bean;

import java.util.Date;

/**
 * 实时预警报警数据
 * 
 * @author JayzeeZhang
 * @date 2018年3月29日
 */
public class RealtimeAlarm {

    private Integer deviceId;
    private Date eventTime;
    private Integer eventId;
    private String currentValue;
    private Integer customerId;
    private Integer isGroup;
    private Float thresholdValue;
    
    public RealtimeAlarm(Integer deviceId, Date eventTime, Integer eventId, String currentValue, Integer customerId,
            Integer isGroup, Float thresholdValue) {
        super();
        this.deviceId = deviceId;
        this.eventTime = eventTime;
        this.eventId = eventId;
        this.currentValue = currentValue;
        this.customerId = customerId;
        this.isGroup = isGroup;
        this.thresholdValue = thresholdValue;
    }
    
    public Integer getDeviceId() {
        return deviceId;
    }
    public void setDeviceId(Integer deviceId) {
        this.deviceId = deviceId;
    }
    public Date getEventTime() {
        return eventTime;
    }
    public void setEventTime(Date eventTime) {
        this.eventTime = eventTime;
    }
    public Integer getEventId() {
        return eventId;
    }
    public void setEventId(Integer eventId) {
        this.eventId = eventId;
    }
    public String getCurrentValue() {
        return currentValue;
    }
    public void setCurrentValue(String currentValue) {
        this.currentValue = currentValue;
    }
    public Integer getCustomerId() {
        return customerId;
    }
    public void setCustomerId(Integer customerId) {
        this.customerId = customerId;
    }
    public Integer getIsGroup() {
        return isGroup;
    }
    public void setIsGroup(Integer isGroup) {
        this.isGroup = isGroup;
    }

    public Float getThresholdValue() {
        return thresholdValue;
    }

    public void setThresholdValue(Float thresholdValue) {
        this.thresholdValue = thresholdValue;
    }
    
}
