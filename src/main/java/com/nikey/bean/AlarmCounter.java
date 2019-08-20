package com.nikey.bean;

import java.util.Date;

/**
 * 最大需量预警报警计数器
 *
 * @author JayzeeZhang
 * @date 2018年3月29日
 */
public class AlarmCounter {

    private Integer powerId;
    private Date insertTime;
    private Integer eventId;
    private Integer count;
    private Integer customerId;
    private Integer timeType;
    private Integer deviceId;

    public AlarmCounter() {

    }

    public AlarmCounter(Integer powerId, Date insertTime, Integer eventId, Integer count, Integer customerId,
                        Integer timeType, Integer deviceId) {
        super();
        this.powerId = powerId;
        this.insertTime = insertTime;
        this.eventId = eventId;
        this.count = count;
        this.customerId = customerId;
        this.timeType = timeType;
        this.deviceId = deviceId;
    }

    public Integer getPowerId() {
        return powerId;
    }

    public void setPowerId(Integer powerId) {
        this.powerId = powerId;
    }

    public Date getInsertTime() {
        return insertTime;
    }

    public void setInsertTime(Date insertTime) {
        this.insertTime = insertTime;
    }

    public Integer getEventId() {
        return eventId;
    }

    public void setEventId(Integer eventId) {
        this.eventId = eventId;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public Integer getCustomerId() {
        return customerId;
    }

    public void setCustomerId(Integer customerId) {
        this.customerId = customerId;
    }

    public Integer getTimeType() {
        return timeType;
    }

    public void setTimeType(Integer timeType) {
        this.timeType = timeType;
    }

    public Integer getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(Integer deviceId) {
        this.deviceId = deviceId;
    }

    public void increaseCount() {
        this.count++;
    }

}
