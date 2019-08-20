package com.nikey.thread;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nikey.bean.PostData;
import com.nikey.util.HostNameUtil;
import com.nikey.util.JsonUtil;
import com.nikey.util.PropUtil;
import com.nikey.util.RedisUtil;
import com.nikey.util.ServiceHelper;

import redis.clients.jedis.Jedis;

/**
 * 写入到remote redis失败的数据缓存在MySQL，此线程用于将数据重新查询出来，写回remote redis
 * 
 * @author JayzeeZhang
 * @date 2017年11月21日
 */
public class HbaseDataToleranter extends Thread {
	
	private final Logger logger = LoggerFactory.getLogger(getClass());
	
	public static void main(String[] args) {
		String arr [] = new String[3];
		List<String> list = new ArrayList<>();
		list.add("1");
		list.add("2");
		list.add("3");
		list.toArray(arr);
		System.out.println(JsonUtil.toJson(arr));
	}
	
	@Override
	public void run() {
		try {
			Thread.sleep(30000l);
			// 确保mysql连接已就绪
			logger.info("start up hbase data toleranter thread worker ...");
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		
		if (HostNameUtil.isAliyun()) {
			Jedis remote_jedis = RedisUtil.getInstance(PropUtil.getString("redis_remote_port"));
			Jedis local_jedis = RedisUtil.getInstance();
			Long front_num = 0l;
			int limit_num = 10;
			
			while (! RedisUtil.ping(local_jedis)) {
				try {
					Thread.sleep(3000l);
					logger.error("connect to local redis error ...");
				} catch (Exception e) {}
			}
			String temp = RedisUtil.get(PropUtil.getString("redis_front_num"), local_jedis);
			if (temp != null) front_num = Long.valueOf(temp);
			temp = RedisUtil.get(PropUtil.getString("redis_limit_num"), local_jedis);
			if (temp != null) limit_num = Integer.valueOf(temp);
			
			while (true) {
				try {
					if (RedisUtil.ping(remote_jedis)) {
						List<PostData> list = ServiceHelper.instance().getMonitordataService().getHbaseData(front_num, limit_num);
						if (list != null && list.size() > 0) {
							String arr [] = new String[list.size()];
							for (int i = 0; i < arr.length; i++) {
								arr[i] = list.get(i).getJson();
							}
							if (RedisUtil.rpush(remote_jedis, PropUtil.getString("redis_tolerant"), arr) != null) {
								front_num = list.get(arr.length - 1).getId();
								RedisUtil.set(local_jedis, PropUtil.getString("redis_front_num"), front_num.toString());
								logger.info("write to remote redis successfully ...");
							} else {
								logger.info("write to remote redis failed ...");
							}
						} else {
							Thread.sleep(30000l); 
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

}
