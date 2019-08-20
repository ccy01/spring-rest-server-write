package com.nikey.thread;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.nikey.util.PropUtil;

/**
 * @author jayzee
 * @date 25 Sep, 2014
 *	thread pool manager
 */
public class ThreadPoolManagerWrite {
	
	/**
	 * thread numbers
	 */
	private final int THREADNUMS = PropUtil.getInt("thread_num");
	
	/**
	 * executor service
	 */
	private final ExecutorService executorService = Executors.newFixedThreadPool(THREADNUMS);
	private final ExecutorService service = Executors.newFixedThreadPool(THREADNUMS);
	/**
	 * singleton
	 */
	private static final ThreadPoolManagerWrite instance = new ThreadPoolManagerWrite();
	public static ThreadPoolManagerWrite instance() {
		return instance;
	}
	private ThreadPoolManagerWrite() {
		for(int i=0; i<THREADNUMS; i++) {
			executorService.execute(new Worker(executorService));
			service.execute(new RealtimeDataWorker(service));
		}
		// 将时间处理独立出来
		new TimerWorker().start();
		// 将redis处理部分独立出来
		new RedisWorker().start();
		new RedisWorker().start();
		// 将remote redis容错逻辑独立出来
		new HbaseDataToleranter().start();
	}

}
