package com.nikey.bean;

/**
 * 最大需量
 * 
 * @author JayzeeZhang
 * @date 2018年3月28日
 */
public class MaxDemand {
	
	private Integer powerId;
	private String time;
	private Float real;
	private Float contract;
	
	public MaxDemand(Integer powerId, String time, Float real, Float contract) {
		this.time = time;
		this.real = real;
		this.contract = contract;
		this.powerId = powerId;
	}
	
	public String getTime() {
		return time;
	}
	public void setTime(String time) {
		this.time = time;
	}
	public Float getReal() {
		return real;
	}
	public void setReal(Float real) {
		this.real = real;
	}
	public Float getContract() {
		return contract;
	}
	public void setContract(Float contract) {
		this.contract = contract;
	}

	public Integer getPowerId() {
		return powerId;
	}

	public void setPowerId(Integer powerId) {
		this.powerId = powerId;
	}
	
}
