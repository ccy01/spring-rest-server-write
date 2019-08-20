package com.nikey.http;

import org.apache.http.NameValuePair;

public class ParameterNameValuePair implements NameValuePair {
	private String paraName="";
	private String paraValue="";
	
	public ParameterNameValuePair(String paraName,String paraValue) {
		this.paraName=paraName;
		this.paraValue=paraValue;
	}

	@Override
	public String getName() {
		return paraName;
	}

	@Override
	public String getValue() {
		return paraValue;
	}

}
