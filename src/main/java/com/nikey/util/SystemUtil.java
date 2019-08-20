package com.nikey.util;

/**
 * java system 工具类
 * 
 * @author jtb
 *
 */
public class SystemUtil {
	
	private static String DEV_MODE = "dev";
	private static String DEV_VM_PARAMETER = "spring.profiles.active";
	
	public static boolean isDev() {
		// TODO 命令行下，mvn以dev模式启动spring boot
		return DEV_MODE.equals(System.getProperty(DEV_VM_PARAMETER));
	}

}
