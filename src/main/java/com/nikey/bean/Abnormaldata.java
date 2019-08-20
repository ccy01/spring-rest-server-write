package com.nikey.bean;

public class Abnormaldata {
	
	private int deviceId;
	private int eventType;
	private String startTime;
	private String endTime;
	private String currenctValue;
	private Float duration;
	
    private Float value0, value1, value2;
    private Long startLong, endLong;
	
	public Float getValue0() {
        return value0;
    }
    public void setValue0(Float value0) {
        this.value0 = value0;
    }
    public Float getValue1() {
        return value1;
    }
    public void setValue1(Float value1) {
        this.value1 = value1;
    }
    public Float getValue2() {
        return value2;
    }
    public void setValue2(Float value2) {
        this.value2 = value2;
    }
	public int getDeviceId() {
		return deviceId;
	}
	public void setDeviceId(int deviceId) {
		this.deviceId = deviceId;
	}
	public int getEventType() {
		return eventType;
	}
	public void setEventType(int eventType) {
		this.eventType = eventType;
	}
	public String getStartTime() {
		return startTime;
	}
	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}
	public String getEndTime() {
		return endTime;
	}
	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}
	public String getCurrenctValue()
    {
        return currenctValue;
    }
    public void setCurrenctValue(String currenctValue)
    {
        this.currenctValue = currenctValue;
    }
    public Float getDuration() {
		return duration;
	}
	public void setDuration(Float duration) {
		this.duration = duration;
	}
    public Long getStartLong() {
        return startLong;
    }
    public void setStartLong(Long startLong) {
        this.startLong = startLong;
    }
    public Long getEndLong() {
        return endLong;
    }
    public void setEndLong(Long endLong) {
        this.endLong = endLong;
    }

}
