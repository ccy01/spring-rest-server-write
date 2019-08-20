package com.nikey.thread;

import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

import com.google.gson.reflect.TypeToken;
import com.nikey.hbase.HbaseTablePool;
import com.nikey.util.DateUtil;
import com.nikey.util.JsonUtil;
import com.nikey.util.LogJsonUtil;
import com.nikey.util.PropUtil;
import com.nikey.util.RedisUtil;
import com.nikey.util.SendEmail;

/**
 * nccserver传输过来的数据处理
 * @author JayzeeZhang
 */
public class RedisWorker extends Thread {
	
	private final Logger logger = LoggerFactory.getLogger(getClass());
	
	private final Jedis jedis = RedisUtil.getInstance();
	private final String redisPost = PropUtil.getString("redis_post");
	
	@Override
	public void run() {		
		work();
	}
	
	@SuppressWarnings("unchecked")
	private void work() {
		// get the hostname
		InetAddress addr = null;
		try {
			addr = InetAddress.getLocalHost();
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		String hostName = null;
		if (addr != null) {
			hostName = addr.getHostName().toString();
		}
		
		int counter = 0;
		Type type = new TypeToken<Map<String, String[]>>(){}.getType();
		while(! Thread.currentThread().isInterrupted() && ! WorkQueue.instance().getStopWorking()) {
			counter++;
			try {
				if (! HbaseTablePool.instance().getIsConnected()) {
					Thread.sleep(1000l);
					continue;
				}
				// 取回写入redis异常的数据
				String value = RedisUtil.lpop(PropUtil.getString("redis_tolerant"), 
						WorkQueue.instance().getLorJedis());
				if (value != null) {
					logger.info("consuming data ...");
					WorkQueue.instance().put((Map<String, String[]>) JsonUtil.fromJsonAsGeneric(value, type));
					value = RedisUtil.lpop(PropUtil.getString("redis_tolerant"), 
							WorkQueue.instance().getLorJedis());
				}
				postDataHandle();
				Thread.sleep(500);
				if (counter > 100) {
					logger.info("i'm alive ...");
					counter = 0;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		// 将queue的数据写入到redis
		if (WorkQueue.instance().getStopWorking() || RedisUtil.get("hbase_connection_error", jedis) != null) {
			Map<String, String[]> value = WorkQueue.instance().takeNoneBlock(); 
			while (value != null) {
				// 获取锁，将当前失败数据写入redis
				RedisUtil.rpush(WorkQueue.instance().getLorJedis(), PropUtil.getString("redis_tolerant"), 
						JsonUtil.toJson(value));
				value = WorkQueue.instance().takeNoneBlock(); 
			}
			// 发邮件告知异常（间隔5分钟发送一次邮件，防止异常时的邮件轰炸）
			String lastError = RedisUtil.get("last_write_hbase_error", jedis);
			if (lastError == null || System.currentTimeMillis() - Long.valueOf(lastError) > 5 * 60 * 1000l) {
				SendEmail.sendMail(hostName + " : POST_HBASE_WRITE_ERROR_" + DateUtil.formatToHHMMSS(new Date()), 
						"SpringRestServerWrite write to hbase error, may be the connection issue ...");
				RedisUtil.set(jedis, "last_write_hbase_error", System.currentTimeMillis() + "");
			}
			// 清空失败状态
			RedisUtil.del("hbase_connection_error", jedis);
			// 休眠足够时间，确保worker已经将错误数据写回redis
			try {
				Thread.sleep(PropUtil.getInt("job_timeout_second") * 1000l);
				logger.info("program will exit after {} seconds ...", PropUtil.getInt("job_timeout_second"));
			} catch (Exception e) {}
			// 退出程序
			System.exit(0);
		}
		logger.info("thread ending ...");
	}

	/**
	 * @return true表示队列中还有数据，false表示无数据
	 */
	private synchronized Boolean postDataHandle() {
		String value = null;
		try {
			value = RedisUtil.lpop(redisPost, jedis);
			if(value != null) {
				String args1 [] = value.split("&");
				if(args1 != null) {
					Map<String, List<String>> preResult = new HashMap<>();
					for(String temp : args1) {
						String param1 [] = temp.split("=");
						List<String> list1 = preResult.get(param1[0]);
						if(list1 == null) {
							list1 = new ArrayList<String>();
						}
						try {
							list1.add(param1[1]);
						} catch (Exception e) {
							e.printStackTrace();
							logger.info(JsonUtil.toJson(param1));
							return false;
						}
						preResult.put(param1[0], list1);
					}
					Map<String, String[]> realResult = new HashMap<>();
					for(String temp : preResult.keySet()) {
						List<String> param1 = preResult.get(temp);
						String list1 [] = new String[param1.size()];
						for(int i=0; i<list1.length; i++) {
							list1[i] = param1.get(i);
						}
						realResult.put(temp, list1);
					}
					try {
						postResultMapHandle(realResult);
					} catch (Exception e) {
						e.printStackTrace();
						logger.info(value);
					}					
				}
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.info(value);
		}		
		return false;
	}
	
	/**
	 * 从redis队列获取post过来的数据
	 * @param realResult
	 */
	private void postResultMapHandle(Map<String, String[]> realResult) {
		String htable = realResult.get(PropUtil.getString("LIB_CURVE_VALUE_TYPE"))[0];
        Map<String, String[]> requestMap = new HashMap<String, String[]>(realResult);
		if(htable != null) {
		    // data_type
			htable = htable.toLowerCase();
			if (! htable.contains("json")) { 
				// data_id
				String data_id = realResult.get("DeviceId") != null ? 
						realResult.get("DeviceId")[0] : realResult.get("GroupId")[0];
	            // data_time
	            String data_time = realResult.get("InsertTime") != null ?
	            		realResult.get("InsertTime")[0] : 
	            			(realResult.get("HappenTime") != null ? 
	            					realResult.get("HappenTime")[0] :
	            						realResult.get("DemandTime")[0]);
	            if (data_time != null) {
	                try {
	                    data_time = DateUtil.formatToHHMMSS(Long.valueOf(data_time) * 1000l);
	                } catch (Exception e) {}
	            }
	            // 调试输出及异常data_id丢弃
	            logger.info(String.valueOf(htable) + ", " + data_id + ", " + data_time);
	            if ("0".equals(data_id)) {
	                logger.error("data_id error : " + JsonUtil.toJson(requestMap));
	                return;
	            }
			} else {
	            // json类数据预处理
	            if (jsonPreHandle(htable, requestMap, "json") == 1) {
	                return;
	            }
	            if (jsonPreHandle(htable, requestMap, "jsonr") == 1) {
	                return;
	            }
			}
			
//			logger.info("before put ...");
			WorkQueue.instance().put(requestMap);
//			logger.info("after put ...");
		} else {
			LogJsonUtil.errorJsonFileRecord(getClass().getSimpleName(), "the htable post from nccserver is null", JsonUtil.toJson(requestMap));
		}
	}

    /**
     * json预处理
     * @param response
     * @param htable
     * @param requestMap
     * @param jkey
     * @return 1失败 0成功
     */
    private int jsonPreHandle(String htable,
            Map<String, String[]> requestMap, String jkey) {
        if(requestMap.get(jkey) != null && requestMap.get(jkey).length > 0) {
            String json = requestMap.get(jkey)[0];
            if(json != null && json.lastIndexOf("}") != -1 && json.indexOf("{") != -1) {
                if ("pointinfochange".equals(htable)) {
                    json = new String(json.getBytes(Charset.forName("iso8859-1")), Charset.forName("UTF-8")); // 中文编码处理                        
                }
                try {
                    String substr = getJsonString(json);
                    Map<String, Object> jmap = JsonUtil.fromJsonToHashMap(substr);
                    requestMap.put(jkey, new String[]{ substr }); // set the correct json back
                    logJsonData(htable, jmap);
                } catch (Exception e) {
                    LogJsonUtil.errorJsonFileRecord(getClass().getSimpleName(), "malformed json", json);
                    return 1;
                }
            }
        }
        return 0;
    }

    /**
     * malform json prehandle
     * @param json
     * @return
     */
    private String getJsonString(String json) {
		int counter = 3; // 推进3次
    	int from = json.indexOf("{");
    	int to = json.lastIndexOf("}") + 1;
    	while (from >= 0 && to > from && counter > 0) {
    		counter--;
    		String jv = json.substring(from, to);
    		try {
    			JsonUtil.fromJsonToHashMap(jv);
    			return jv;
			} catch (Exception e) {
				json = json.substring(from, to - 1); // 向后推进一个字符
				from = json.indexOf("{");
				to = json.lastIndexOf("}") + 1;
			}
    	}
    	return null;
	}

	/**
     * 打印json类数据的调试信息
     * @param htable
     * @param jmap
     */
    private void logJsonData(String htable, Map<String, Object> jmap) {
        StringBuffer sb = new StringBuffer();
        if ("pointinfochange".equals(htable)) {
            sb.append(htable);
        } else {
            if (jmap.get("data_type") != null) {
                sb.append(jmap.get("data_type").toString());
                Object data_id = jmap.get("device_id") != null ? jmap.get("device_id") : jmap.get("group_id");
                if (data_id != null) {
                    sb.append(", " + data_id.toString());
                    Object data_time = null;
                    if (jmap.get("insert_time") != null) {
                        data_time = jmap.get("insert_time");
                    } else if (jmap.get("happen_time") != null) {
                        data_time = jmap.get("happen_time");
                    } else if (jmap.get("change_time") != null) {
                        data_time = jmap.get("change_time");
                    }
                    if (data_time != null) {
                        try {
                            Long time = 1000l * Long.valueOf(data_time.toString());
                            sb.append(", " + DateUtil.formatToHHMMSS(time));
                        } catch (Exception e) {
                            try {
                            	Long time = 1000l * Double.valueOf(data_time.toString()).longValue();
                                sb.append(", " + DateUtil.formatToHHMMSS(time));
							} catch (Exception e2) {}
                        }
                    }
                }
            }
        }
        if (sb.length() != 0) {
            logger.info(sb.toString());
        }
    }

}
