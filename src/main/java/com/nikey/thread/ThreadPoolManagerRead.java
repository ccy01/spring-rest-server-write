package com.nikey.thread;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nikey.util.PropUtil;

/**
 * @author jayzee
 * @date 25 Sep, 2014
 *	thread pool manager
 */
public class ThreadPoolManagerRead {
	
	/**
	 * slfj
	 */
	Logger logger = LoggerFactory.getLogger(getClass());
	
	/**
	 * thread numbers
	 */
	private final int THREADNUMS = PropUtil.getInt("thread_num_read");
	private int FRONT_THREAD_SIZE = 0;
	
	/**
	 * lock for ThreadPoolManager
	 */
	private final Lock lock = new ReentrantLock();
	
	/**
	 * executor service
	 */
	private final ExecutorService executorService = Executors.newFixedThreadPool(THREADNUMS);
	
	/**
	 * singleton
	 */
	private static final ThreadPoolManagerRead instance = new ThreadPoolManagerRead();
	private ThreadPoolManagerRead() {}
	public static ThreadPoolManagerRead instance() {
		return instance;
	}
	
	/**
	 * @date 26 Sep, 2014
	 * @param callable
	 * @param timeout
	 * @return ResultScanner
	 *	submit the scan job and wait for the result in timeout time
	 */
	public ResultScanner submit(Callable<ResultScanner> callable, int timeout_second) {
		ResultScanner result = null;
		
		/**
		 * submit the job to executor
		 */
		Future<ResultScanner> future = null;
		try {
			lock.lock();
			if(FRONT_THREAD_SIZE >= THREADNUMS) return null; // the thread pool is full
			else {
				future = executorService.submit(callable);
				FRONT_THREAD_SIZE++; // add thread size
			}			
		} finally{
			lock.unlock();
		}
		
		if(future != null) {
			try {
				/**
				 * wait for the result
				 */
				result = future.get(timeout_second, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				logger.warn(PropUtil.getString("WARN0001"), Thread.currentThread().getName());
			} catch (ExecutionException e) {
				logger.warn(PropUtil.getString("WARN0002"), Thread.currentThread().getName());
			} catch (TimeoutException e) {
				logger.warn(PropUtil.getString("WARN0003"), Thread.currentThread().getName());
			} finally {
				try {
					lock.lock();
					FRONT_THREAD_SIZE--; // sub thread size
				} finally{
					lock.unlock();
				}
			}
		}
		
		return result;
	}
	
	/**
	 * @date 27 Sep, 2014
	 * @param callables
	 * @param timeout_second
	 * @return List<ResultScanner>
	 *	submit multi-job, all successful or all failure
	 */
	public Queue<ResultScanner> submitMultiJob(List<ReadWorker> callables, int timeout_second) {
		Queue<ResultScanner> results = new LinkedBlockingDeque<>();
		
		try {
			lock.lock();
			if((FRONT_THREAD_SIZE + callables.size()) > THREADNUMS) {
				return null; // thread pool is full
			}
		} finally{
			lock.unlock();
		}
		
		for(Callable<ResultScanner> callable : callables) {
			/**
			 * submit the job to executor
			 */
			Future<ResultScanner> future = null;
			try {
				lock.lock();
				future = executorService.submit(callable);
				FRONT_THREAD_SIZE++; // add thread size		
			} finally{
				lock.unlock();
			}
			
			if(future != null) {
				try {
					/**
					 * wait for the result
					 */
					ResultScanner result = future.get(timeout_second, TimeUnit.SECONDS);
					results.add(result);
				} catch (InterruptedException e) {
					logger.warn(PropUtil.getString("WARN0001"), Thread.currentThread().getName());
					return null; // error occurs
				} catch (ExecutionException e) {
					logger.warn(PropUtil.getString("WARN0002"), Thread.currentThread().getName());
					return null; // error occurs
				} catch (TimeoutException e) {
					logger.warn(PropUtil.getString("WARN0003"), Thread.currentThread().getName());
					return null; // error occurs
				} finally {
					try {
						lock.lock();
						FRONT_THREAD_SIZE--; // sub thread size
					} finally{
						lock.unlock();
					}
				}
			}					
		}
		return results;
	}
	
	/**
	 * @date 27 Sep, 2014
	 * @return int
	 *	get the pool' active count
	 */
	public boolean isBusyWorking() {
		try {
			lock.lock();
			if(FRONT_THREAD_SIZE >= THREADNUMS) return true;
			else return false;
		} finally{
			lock.unlock();
		}
	}

}
