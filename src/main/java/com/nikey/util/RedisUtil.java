package com.nikey.util;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

/**
 * @author Jayzee
 * @date 2017年6月5日 下午3:31:01
 * 操作redis的工具类，使用同步保证线程安全，使用说明：
 * 1. 在业务类声明成员变量：private Jedis jedis = RedisUtil.getInstance();
 * 2. 调用相应方法：RedisUtil.get("1/monitordata/1", jedis)
 */
public class RedisUtil {
	
	private static final Logger logger = LoggerFactory.getLogger(RedisUtil.class);
	
    /**
     * 创建jedis实例
     * 
     * @return Jedis
     */
    public static Jedis getInstance() {
        Jedis jedis = new Jedis(PropUtil.getString("redis_host"),
                PropUtil.getInt("redis_port"));
        if(jedis != null && PropUtil.getString("redis_passwd") != null) {
            jedis.auth(PropUtil.getString("redis_passwd"));
        }
        return jedis;
    }

	/**
	 * 指定port连接redis，默认host为127.0.0.1
	 * 
	 * @param port
	 * @return Jedis
	 */
	public static Jedis getInstance(String port) {
        Jedis jedis = new Jedis("127.0.0.1",
                Integer.valueOf(port));
        if(jedis != null && PropUtil.getString("redis_passwd") != null) {
            jedis.auth(PropUtil.getString("redis_passwd"));
        }
        return jedis;
    }

    /**
     * 根据key获取value
     * @param key
     * @param jedis
     * @return String key对应的value
     */
    public static String get(String key, Jedis jedis) {
        String value = null;
        synchronized (jedis) {
            try {
                value = jedis.get(key);
                
                //有需要时打印相关日志
                File file = new File("/home/gx/tomcat/switch.txt");
                if (file.exists()) {
                	System.out.println("key = " + key);
					System.out.println(value);
				}
            } catch (Exception e) { // will auto re-connect
                try {
                    jedis.close();
                    jedis.connect();
                    value = jedis.get(key);
                } catch (Exception e2) {
                    logger.error("获取出错！，链接失败或未知错误！");
                	return "-1";
                }
            }
        }
        return value;
    }
    
    /**
     * 删除指定key
     * @param key
     * @param jedis
     * @return 删除的结果数量
     */
    public static Long del(String key, Jedis jedis) {
        Long value = null;
        synchronized (jedis) {
            try {
                value = jedis.del(key);
            } catch (Exception e) { // will auto re-connect
                try {
                    jedis.close();
                    jedis.connect();
                    value = jedis.del(key);
                } catch (Exception e2) {}
            }
        }
        return value;
    }
    
    /**
     * @desc 插入实时刷新数据
     */
    public static void setKeyData(String key, Jedis jedis, String value) {
    	/*boolean isKey = jedis.exists(key);
    	System.out.println("key exists is " + isKey);
    	if (isKey) {
			set(jedis, key, value);
		}else {
			setnx(jedis, key, value);
		}*/
    	set(jedis, key, value);
	}
    
    /**
     * @desc 将实时曲线值插入到redis中
     */
    public static void setKey(String key, Jedis jedis, String newValue) {
    	String value = get(key, jedis);
    	
    	//将传入的值转为哈希格式以便调用
		Map<String, Object> newMap = toListJson(newValue);
		String data = JsonUtil.toJson(newMap);
		
		Map<String, Object> oldMap = JsonUtil.fromJsonToHashMap(value);
		double dbTime = (double) oldMap.get("lastTime");
		long lastTime = (long) dbTime;
		long newTime = (long) newMap.get("lastTime");
		
		String lastDate = DateUtil.formatToYYMMDD(lastTime);
		String newDate = DateUtil.formatToYYMMDD(newTime);
		
		//若处于同一天则数据追加，否则覆盖该value
		if (newDate.equals(lastDate)) {
			data = handleDataByTimes(newMap, oldMap, lastTime);
		}/*else {
			System.out.println("------start------");
			System.out.println("newDate = " + newDate + " lastDate = " + lastDate);
			System.out.println("newValue = " + newValue);
			System.out.println("data = " + data);
			System.out.println("------end------");
		}*/
		set(jedis, key, data);
		
	}
    
    //设置key值的方法
    public static void set(Jedis jedis, String key, String value) {
    	synchronized (jedis) {
			try {
				jedis.set(key, value);
			} catch (Exception e) {
				try {
					jedis.close();
					jedis.connect();
					jedis.set(key, value);
				} catch (Exception e2) {
					logger.error("set to redis error, key is {}, value is {}", new String [] {key, value});
				}
			}
		}
	}
    
    //辅助方法，将传入的数据转为链表格式，再转为json格式返回
    private static Map<String, Object> toListJson(String data) {
    	
    	Map<String, Object> resultMap = new HashMap<String, Object>();
    	
    	Map<String, Object> valueMap = JsonUtil.fromJsonToHashMap(data);
		double dbTime = (double) valueMap.get("insert_time");
		long lastTime = (long) dbTime * 1000l;
		for(String key : valueMap.keySet()) {
			if (!key.equals("insert_time") && !key.equals("data_type")
					&& !key.equals("type") && !key.equals("device_id")) {
				Double value = (double) valueMap.get(key);
				
				//传入的值为负数时不写入
				if (value < 0) {
					continue; 
				}
				
//				Object[] objValue = new Object[] {value, lastTime};
				List<Object> pList = new ArrayList<Object>();
				List<Object> objValue = new ArrayList<Object>();
				objValue.add(lastTime);
				objValue.add(value);
				
				//二维数组
				pList.add(objValue);
				
				resultMap.put(key, pList);
			}else if (key.equals("insert_time")) {
				resultMap.put("lastTime", lastTime);
			}
		}
		
		return resultMap;
	}
    
    //辅助方法，封装对于不同时刻插入的处理
	@SuppressWarnings("unchecked")
	private static String handleDataByTimes(Map<String, Object> newMap, Map<String, Object> oldMap, long lastTime) {
    	Map<String, Object> resultMap = new HashMap<String, Object>();
    	long nowTime = (long) newMap.get("lastTime");
    	//当lastTime与当前时间相差不超过十分钟则追加到数据链表结尾部分
    	System.out.println(nowTime - lastTime <= 600000);
		for(String item : oldMap.keySet()) {
			if (!item.equals("lastTime")) {
				Object itemValue = oldMap.get(item);
				if (itemValue != null) {
					List<Object> objList = (List<Object>) oldMap.get(item);
					Object dataMapItem = newMap.get(item);
					
					if (nowTime - lastTime > 600000) {
						//插入断点，断点的时间为旧数据的lastTime加多两分钟
						List<Long> nullPoint = new ArrayList<Long>();
						nullPoint.add(lastTime + 120000);
						nullPoint.add(null);
						objList.add(nullPoint);
					}
					
					if (dataMapItem != null) {
						//追加到数据结尾部分
						List<Object> dataList = (List<Object>) dataMapItem;
						objList.add(dataList.get(0));
					}
					
					resultMap.put(item, objList);
				}
				
			}else {
				//将新数据的lastTime写入
				resultMap.put(item, newMap.get(item));
			}
			
		}
		String result = JsonUtil.toJson(resultMap);
		return result;
	}
    
    /**
     * Returns all the keys matching the glob-style pattern as space separated strings. 
     * For example if you have in the database the keys "foo" and "foobar" the command "KEYS foo*" will return "foo foobar".
     * @param pattern
     * @param jedis
     * @return Set<String>
     */
    public static Set<String> keys(String pattern, Jedis jedis) {
        Set<String> value = null;
        synchronized (jedis) {
            try {
                value = jedis.keys(pattern);
            } catch (Exception e) { // will auto re-connect
                try {
                    jedis.close();
                    jedis.connect();
                    value = jedis.keys(pattern);
                } catch (Exception e2) {}
            }
        }
        return value;
    }
    
    /**
     * pop the first String in list
     * @param key
     * @return String
     */
    public static String lpop(String key, Jedis jedis) {
        String value = null;
        synchronized (jedis) {
            try {
                value = jedis.lpop(key);
            } catch (Exception e) { // will auto re-connect
                try {
                    jedis.close();
                    jedis.connect();
                    value = jedis.lpop(key);
                } catch (Exception e2) {}
            }
        }
        return value;
    }
    
    /**
     * 计算redis链表的长度
     * @param key
     * @param jedis
     * @return Long
     */
    public static Long llen(String key, Jedis jedis) {
        Long value = null;
        synchronized (jedis) {
            try {
                value = jedis.llen(key);
            } catch (Exception e) { // will auto re-connect
                try {
                    jedis.close();
                    jedis.connect();
                    value = jedis.llen(key);
                } catch (Exception e2) {}
            }
        }
        return value;
    }
    
    /**
     * 将valueStr添加到队列末尾
     * @param key
     * @param valueStr
     * @param jedis
     * @return Long
     */
    public static Long rpush(Jedis jedis, String key, String ...valueStr) {
        Long value = null;
        synchronized (jedis) {
            try {
            	
                value = jedis.rpush(key, valueStr);
            } catch (Exception e) { // will auto re-connect
                try {
                    jedis.close();
                    jedis.connect();
                    value = jedis.rpush(key, valueStr);
                } catch (Exception e2) {}
            }
        }
        return value;
    }

	/**
	 * ping-pong
	 * 
	 * @param remote_jedis
	 */
	public static boolean ping(Jedis jedis) {
		try {
			jedis.ping();
			return true;
		} catch (Exception e) { // will auto re-connect
            try {
                jedis.close();
                jedis.connect();
                jedis.ping();
                return true;
            } catch (Exception e2) {}
        }
		return false;
	}
	
	/**
	 * hkeys hash
	 * 
	 * @param key
	 * @param jedis
	 * @return
	 */
	public static Set<String> hkeys(String key, Jedis jedis) {
		try {
			return jedis.hkeys(key);
		} catch (Exception e) { // will auto re-connect
            try {
                jedis.close();
                jedis.connect();
                return jedis.hkeys(key);
            } catch (Exception e2) {}
        }
		return null;
	}
	
	/**
	 * hdel hash keys
	 * 
	 * @param jedis
	 * @param key
	 * @param fields
	 */
	public static void hdel(Jedis jedis, String key, String ...fields) {
		try {
			jedis.hdel(key, fields);
		} catch (Exception e) {
			try {
				jedis.close();
	            jedis.connect();
	            jedis.hdel(key, fields);
			} catch (Exception e2) {}
		}
	}
	
	/**
	 * hgetall hash
	 * 
	 * @param jedis
	 * @param key
	 * @return
	 */
	public static Map<String, String> hgetAll(Jedis jedis, String key) {
		try {
			return jedis.hgetAll(key);
		} catch (Exception e) {
			try {
				jedis.close();
	            jedis.connect();
	            return jedis.hgetAll(key);
			} catch (Exception e2) {}
		}
		return null;
	}

}
