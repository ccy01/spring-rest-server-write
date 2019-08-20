package com.nikey.bean;

/**
 * 电源信息
 * 
 * @author JayzeeZhang
 * @date 2018年3月27日
 */
public class PowerInfo {

	private Integer powerId;
	private Integer customerId;
	private Integer deviceId;
	private Integer isGroup;
	
	public Integer getPowerId() {
		return powerId;
	}
	public void setPowerId(Integer powerId) {
		this.powerId = powerId;
	}
	public Integer getCustomerId() {
		return customerId;
	}
	public void setCustomerId(Integer customerId) {
		this.customerId = customerId;
	}
	public Integer getDeviceId() {
		return deviceId;
	}
	public void setDeviceId(Integer deviceId) {
		this.deviceId = deviceId;
	}
	public boolean getIsGroup() {
		return isGroup == 1;
	}
	public void setIsGroup(Integer isGroup) {
		this.isGroup = isGroup;
	}
	
}
