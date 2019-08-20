package com.nikey.bean;

/**
 * 缓存在MySQL中的post数据 
 * 
 * @author JayzeeZhang
 * @date 2017年11月21日
 */
public class PostData {

	private Long id;
	private String json;
	
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getJson() {
		return json;
	}
	public void setJson(String json) {
		this.json = json;
	}
	
}
