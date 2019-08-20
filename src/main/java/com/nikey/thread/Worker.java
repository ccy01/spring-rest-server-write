package com.nikey.thread;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nikey.hbase.HTableMapper;
import com.nikey.hbase.HTableMapperFactory;
import com.nikey.hbase.HbaseTablePool;
import com.nikey.redis.RedisMapper;
import com.nikey.redis.RedisMapperFactory;
import com.nikey.util.ClassNameUtil;
import com.nikey.util.HostNameUtil;
import com.nikey.util.JsonUtil;
import com.nikey.util.PropUtil;
import com.nikey.util.RedisUtil;
import com.nikey.util.ServiceHelper;

import redis.clients.jedis.Jedis;

import com.nikey.util.JobControllUtil;

/**
 * @author jayzee
 * @date 25 Sep, 2014
 *    workers to do some job
 */
public class Worker implements Runnable{
    
    /**
     * slfj
     */
    Logger logger = LoggerFactory.getLogger(getClass());
    
    private final ExecutorService executor;
    
    public Worker(ExecutorService executor) {
        this.executor = executor;
    }

    @Override
    public void run() {
        work();
        // shutdown
        logger.warn("Thread {} has been shut down.", Thread.currentThread().getName());
    }
    
    /**
     * @date 25 Sep, 2014
     *    thread work
     */
    private void work() {
        Jedis remote_jedis = RedisUtil.getInstance(PropUtil.getString("redis_remote_port"));
        int counter = 0;
        
        while(! Thread.currentThread().isInterrupted() && ! WorkQueue.instance().getStopWorking()) {
            try {
                if (! HbaseTablePool.instance().getIsConnected()) {
                    Thread.sleep(1000l);
                    continue;
                }
                // 1. take value from queue
                Map<String, String[]> value = WorkQueue.instance().take();
                if(value != null) {
                    // 2. convert the value to put
                    /**
                     * WARN: request parameter map must contains key <htable>, and its value should not be null,
                     * <htable> is used for reflect the mapper class
                     */
                    String className = ClassNameUtil.constructClassNameForMapper(value);
                    if (className != null) {
                        String[] classNames = className.split("_");
                        if (classNames[1].equals("jsonr")) {
                            final RedisMapper mapper = RedisMapperFactory.instance().getRedisMapper(classNames[0]);
                            if(mapper != null) {
                                try {
                                    //将数据写入到redis中
                                    mapper.convertParameterMapToRedis(value);
                                } catch (Exception e) {
                                    logger.error(JsonUtil.toJson(value));
                                    e.printStackTrace();
                                }                            
                            } else {
                                logger.error("Class " + className + " is not existed(write-error)!");
                            }
                        } else {
                            final HTableMapper mapper = HTableMapperFactory.instance().getHTableMapper(classNames[0]);
                            if(mapper != null) {
                                try {
                                    final List<Put> put = mapper.convertParameterMapToPut(value);
                                    // 3. put it to hbase with timeout
                                    if (put != null && put.size() > 0) {
                                        Callable<Boolean> task = new Callable<Boolean>() {
                                            @Override
                                            public Boolean call() throws Exception {
                                                return mapper.put(put);
                                            }
                                        };
                                       boolean rc = JobControllUtil.submitJobNew(task, value, getClass().getSimpleName());
                                       counter = dataTolerant(remote_jedis, counter, value, rc, classNames[0]);
                                    } else {
                                        if (classNames[0].toLowerCase().contains("alarmdata")) {
                                            if (! HostNameUtil.isAliyun()) {
                                                logger.info("consuming alarmdata x1");
                                            }
                                            counter = dataTolerant(remote_jedis, counter, value, true, classNames[0]);
                                        } else if (  
                                                !classNames[0].toLowerCase().contains("commerr") &&
                                                !classNames[0].toLowerCase().contains("demand")) {
                                            logger.info("mapper return null, json is {}, className is {}", JsonUtil.toJson(value), classNames[0]);
                                        } 
                                    }
                                } catch (Exception e) {
                                    String json = JsonUtil.toJson(value);
                                    if (!json.contains("nan")) {
                                        logger.error(json);
                                    }
                                    e.printStackTrace();
                                }                            
                            } else {
                                logger.error("Class " + className + " is not existed(write-error)!");
                            }
                        }
                        
                    } else {
                        logger.info("The htable's value in request parameter map is null!");
                    }                    
                }
            } catch (InterruptedException e) { // interrupt from service logic
                if(! executor.isShutdown()) {
                    // interrupt
                    logger.warn("Thread {} has been interrupted.", Thread.currentThread().getName());
                    // end the thread's work
                    return;
                }
            }
        }
    }

    private int dataTolerant(Jedis remote_jedis, int counter, Map<String, String[]> value,
            boolean rc, String className) {
        if (!rc) {
            logger.error("write to hbase error ...");
            // 关闭接收ncc数据 && 通知所有Worker停止工作 && 通知维护者，将queue的数据写入到redis
            WorkQueue.instance().setStopWorking(true);
            // 告知aliyun主机，不要再往本地redis写数据了，否则内存可能被撑爆
            RedisUtil.set(WorkQueue.instance().getLorJedis(), "reject_remote", "reject_remote");
            // 获取锁，将当前失败数据写入redis
            RedisUtil.rpush(WorkQueue.instance().getLorJedis(), PropUtil.getString("redis_tolerant"), 
                    JsonUtil.toJson(value));
        } else {
            // 数据写入恢复正常，解除reject_remote
            RedisUtil.del("reject_remote", WorkQueue.instance().getLorJedis());
            // 阿里云主机：将数据写入到remote redis
            if (remote_jedis != null && HostNameUtil.isAliyun()) {
                String json = JsonUtil.toJson(value);
                // 决定是否写数据到云端redis（防止撑爆）
                if (RedisUtil.get("reject_remote", remote_jedis) == null &&
                        RedisUtil.rpush(remote_jedis, PropUtil.getString("redis_tolerant"), json) != null) {
                    // 1. 可写到远端，或远端未联通，不可知
                    // 2. 写到远端成功
                    logger.info("write to remote redis successfully : " + className);
                    counter = 0;
                } else {
                    // 决定是否要写入到本地mysql（防止撑爆）
                    if (RedisUtil.get("stop_mysql", WorkQueue.instance().getLorJedis()) == null) {
                        ServiceHelper.instance().getMonitordataService().writeHbaseData(json);
                        logger.info("remote redis unreachable, write to local mysql ...");
                        counter++;
                        if (counter == 200) {
                            logger.info("write local mysql for 200 times, set stop_mysql to local redis will not cache data to local mysql ...");
                            counter = 0;
                        }
                    }
                }
            }
        }
        return counter;
    }

}
