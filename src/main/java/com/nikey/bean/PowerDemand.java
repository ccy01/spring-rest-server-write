package com.nikey.bean;

import java.util.Date;

/**
 * 电源最大需量设置
 * 
 * @author JayzeeZhang
 * @date 2018年3月27日
 */
public class PowerDemand {
	
	private Integer powerId;
	private Float maxDemand;
	private Date recordDate;
	private Integer isGroup;
	private Integer deviceId;
	
	public Integer getPowerId() {
		return powerId;
	}
	public void setPowerId(Integer powerId) {
		this.powerId = powerId;
	}
	public Float getMaxDemand() {
		return maxDemand;
	}
	public void setMaxDemand(Float maxDemand) {
		this.maxDemand = maxDemand;
	}
	public Date getRecordDate() {
		return recordDate;
	}
	public void setRecordDate(Date recordDate) {
		this.recordDate = recordDate;
	}
	public boolean getIsGroup() {
		return isGroup == 1;
	}
	public void setIsGroup(Integer isGroup) {
		this.isGroup = isGroup;
	}
	public Integer getDeviceId() {
		return deviceId;
	}
	public void setDeviceId(Integer deviceId) {
		this.deviceId = deviceId;
	}

}
