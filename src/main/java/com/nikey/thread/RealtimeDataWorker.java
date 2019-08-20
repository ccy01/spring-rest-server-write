package com.nikey.thread;

import com.nikey.redis.RedisMapper;
import com.nikey.redis.RedisMapperFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * redis实时数据处理
 * 
 * @author JayzeeZhang
 * @date 2018年1月26日
 */
public class RealtimeDataWorker implements Runnable {
	
	private Logger logger = LoggerFactory.getLogger(getClass());

	private ExecutorService service ;

	public RealtimeDataWorker(ExecutorService service){
		this.service = service;
	}
	
	@Override
	public void run() {

		logger.info("start thread -------------------------");
		while (!WorkQueue.instance().getStopWorking() && !Thread.currentThread().isInterrupted()) {

			try {
				Map<String, String[]> value = RealtimeDataWorkerQueue.instance().get();
				//TODO 没有判断非空情况,get()阻塞,返回空
				if(value==null ||value.size()==0) continue;
				String htable = value.get("htable")[0].toLowerCase();
				String className = "com.nikey.redis." + htable.substring(0, 1).toUpperCase() + htable.substring(1) + "RedisMapper";
				RedisMapper redisMapper = RedisMapperFactory.instance().getRedisMapper(className);
				
				if (redisMapper != null) {
					redisMapper.convertParameterMapToRedis(value);
				}
			} catch (InterruptedException e) {
				if(!service.isShutdown()) {
					logger.error("Thread{} thread is interrupt ", Thread.currentThread().getName());
				}
				e.printStackTrace();
			}
		}
	}
	

	
}
