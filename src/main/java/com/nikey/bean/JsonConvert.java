package com.nikey.bean;

public class JsonConvert {

	private String customer;
	private String pointinfo;
	private String time;
	private String event;
	private String threhold;
	private String value;
    
	public JsonConvert() {
	}
	public String getCustomer() {
		return customer;
	}
	public void setCustomer(String customer) {
		this.customer = customer;
	}
	public String getPointinfo() {
		return pointinfo;
	}
	public void setPointinfo(String pointinfo) {
		this.pointinfo = pointinfo;
	}
	public String getTime() {
		return time;
	}
	public void setTime(String time) {
		this.time = time;
	}
	
	public String getEvent() {
		return event;
	}
	public void setEvent(String event) {
		this.event = event;
	}
	public String getThrehold() {
		return threhold;
	}
	public void setThrehold(String threhold) {
		this.threhold = threhold;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	@Override
	public String toString() {
		return "JsonConvert [customer=" + customer + ", pointinfo=" + pointinfo + ", time=" + time + ", event=" + event
				+ ", threhold=" + threhold + ", value=" + value + "]";
	}
    
    
	
}
