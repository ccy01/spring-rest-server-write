package com.nikey.util;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class JobControllUtil {
	
	/*
	 * thread timeout controll
	 */
	private static final ExecutorService executorService = Executors.newCachedThreadPool();
	private static int job_timeout_second = PropUtil.getInt("job_timeout_second");
	
	public static boolean submitJob(Callable<String> task, Object data_tolog, String simpleName) {
		Future<String> future = executorService.submit(task);
		boolean flag = true;
		try {
			future.get(job_timeout_second, TimeUnit.SECONDS);
		} catch (Exception e) {
			future.cancel(true);
			LogJsonUtil.errorJsonFileRecord(simpleName, e.getMessage(),
					JsonUtil.toJson(data_tolog));
			flag = false;
		}
		return flag;
	}
	
	/**
	 * @param task
	 * @param data_tolog
	 * @param simpleName
	 * @return boolean
	 * 返回值为false表示失败，返回值为true表示成功
	 */
	public static boolean submitJobNew(Callable<Boolean> task, Object data_tolog, String simpleName) {
		Future<Boolean> future = executorService.submit(task);
		try {
			return future.get(job_timeout_second, TimeUnit.SECONDS);
		} catch (Exception e) {
			future.cancel(true);
			LogJsonUtil.errorJsonFileRecord(simpleName, e.getMessage(),
					JsonUtil.toJson(data_tolog));
			return false;
		}
	}

}
