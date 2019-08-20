package com.nikey.http;


/*
	static final String SCHEME = "http";  
	static final String HOST = "192.168.0.30";
	static final int PORT = 8080;
	static final String LOG4J_PATH = "log4j.properties";
	//Server paths
   static final String SERVER_HANDLE = "/jasperserver";
   static final String BASE_REST_URL = SERVER_HANDLE+"/rest";
 */
public class HttpClientConfigItem {
	private String scheme;
	private String host;
	private int port;
	private String log4j_path;
	private String server_handle;
	private String base_rest_url;
	
	public String getScheme() {
		return scheme;
	}
	public void setScheme(String scheme) {
		this.scheme = scheme;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public String getLog4j_path() {
		return log4j_path;
	}
	public void setLog4j_path(String log4j_path) {
		this.log4j_path = log4j_path;
	}
	public String getServer_handle() {
		return server_handle;
	}
	public void setServer_handle(String server_handle) {
		this.server_handle = server_handle;
	}
	public String getBase_rest_url() {
		return server_handle+base_rest_url;
	}
	public void setBase_rest_url(String base_rest_url) {
		this.base_rest_url = base_rest_url;
	}
	@Override
	public String toString() {
		return "HttpClientConfigItem[scheme="+scheme+",host="+host+",port="+port+",log4j_path="+log4j_path+
				",server_handle"+server_handle+",server_handle="+server_handle+",base_rest_url="+base_rest_url+"]";
	}

	
}
