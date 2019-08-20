package com.nikey.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * 获取主机名
 * 
 * @author JayzeeZhang
 * @date 2017年12月4日
 */
public class HostNameUtil {
	
	public static void main(String[] args) {
		System.out.println(hostName);
		
	}
	
	private static String hostName = null;
	
	static {
		try {
			InetAddress addr = null;
			try {
				addr = InetAddress.getLocalHost();
			} catch (UnknownHostException e1) {
				e1.printStackTrace();
			}
			hostName = addr.getHostName().toString();
		} catch (Exception e) {}
	}
	
	public static boolean isAliyun() {
		if (hostName != null)
			return hostName.equals(PropUtil.getString("aliyun"));
		else 
			return false;
	}
	
	/**
	 * 是否为容转需主机
	 * 
	 * @return
	 */
	public static boolean isCapacityToDemandServer() {
		if (hostName != null)
			return hostName.equals(PropUtil.getString("CapacityToDemandServer"));
		else 
			return false;
	}

}
