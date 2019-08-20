package com.nikey.bean;

/**
 * 报警接收人
 * 
 * @author Administrator
 *
 */
public class AlarmReceiver {
	
	private Integer companyId;
	private String phoneNumber;
	
	public Integer getCompanyId() {
		return companyId;
	}
	public void setCompanyId(Integer companyId) {
		this.companyId = companyId;
	}
	public String getPhoneNumber() {
		return phoneNumber;
	}
	public void setPhoneNumber(String phoneNumber) {
		this.phoneNumber = phoneNumber;
	}

}
