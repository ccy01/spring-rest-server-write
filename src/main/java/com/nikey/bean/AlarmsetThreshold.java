package com.nikey.bean;

/**
 * 预警报警阈值
 * 
 * @author JayzeeZhang
 * @date 2018年3月29日
 */
public class AlarmsetThreshold {
	
	private Integer deviceId;
	private Integer eventId;
	private Float max;
	private Float min;
	private Integer customerId;
	private Integer powerId;
	private Integer isGroup;
	private String alarmTypeName;
	private Integer alarmEventId;
	
	
	public Integer getDeviceId() {
		return deviceId;
	}
	public void setDeviceId(Integer deviceId) {
		this.deviceId = deviceId;
	}
	public Integer getEventId() {
		return eventId;
	}
	public void setEventId(Integer eventId) {
		this.eventId = eventId;
	}
	public Float getMax() {
		return max;
	}
	public void setMax(Float max) {
		this.max = max;
	}
	public Float getMin() {
		return min;
	}
	public void setMin(Float min) {
		this.min = min;
	}
	public Integer getCustomerId() {
		return customerId;
	}
	public void setCustomerId(Integer customerId) {
		this.customerId = customerId;
	}
	public Integer getPowerId() {
		return powerId;
	}
	public void setPowerId(Integer powerId) {
		this.powerId = powerId;
	}
	public boolean getIsGroup() {
		return isGroup == 1;
	}
	public void setIsGroup(Integer isGroup) {
		this.isGroup = isGroup;
	}
	public String getAlarmTypeName(){
	    	return alarmTypeName;
	}
	public void setAlarmTypeName(String alarmTypeName){
	    	this.alarmTypeName=alarmTypeName;
	}
	public Integer getAlarmEventId() {
		return alarmEventId;
	}
	public void setAlarmEventId(Integer alarmEventId) {
		this.alarmEventId = alarmEventId;
	}
	

}
