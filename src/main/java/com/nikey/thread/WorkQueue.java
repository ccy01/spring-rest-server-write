package com.nikey.thread;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nikey.util.DateUtil;
import com.nikey.util.JsonUtil;
import com.nikey.util.LogJsonUtil;
import com.nikey.util.NumberUtil;
import com.nikey.util.PropUtil;
import com.nikey.util.RedisUtil;
import com.nikey.util.SendEmail;
import com.nikey.util.ServiceHelper;

import redis.clients.jedis.Jedis;

/**
 * @author jayzee
 * @date 25 Sep, 2014
 *
 */
public class WorkQueue {
	
	public static void main(String[] args) {
		System.out.println(instance);
		System.out.println(instance.hashCode());
	}
	
	/**
	 * slfj
	 */
	Logger logger = LoggerFactory.getLogger(getClass());
	
	/**
	 * work queue
	 */
	private final ConcurrentLinkedQueue<Map<String, String[]>> queue = new ConcurrentLinkedQueue<Map<String, String[]>>();
	private volatile boolean stopWorking = false; // default working
	private final Jedis lorJedis = RedisUtil.getInstance(); // jedis to use when write to hbase error
	
	/**
	 * lock & condition for thread workers
	 */
	private final Lock lock = new ReentrantLock();
	private final Condition monitor = lock.newCondition();
	private final Long now = System.currentTimeMillis();
	
	/**
	 * deviceid & time record
	 */
	private final Map<Integer, Long> companyIDAndTimeMaps = new ConcurrentHashMap<Integer, Long>();
	private final Map<Short, Map<Integer, Long>> deviceIDAndTimeMaps = new ConcurrentHashMap<Short, Map<Integer, Long>>();
	
	/**
	 * max size and front size of work queue
	 */
	private final Integer QUEUE_MAX_SIZE = PropUtil.getInt("QUEUE_MAX_SIZE"); 
	private Integer QUEUE_FRONT_SIZE = 0;
	
	/**
	 * singletop pattern
	 */
	private static final WorkQueue instance = new WorkQueue();
	private WorkQueue() {}
	public static WorkQueue instance() {
		return instance;
	}
	
	/**
	 * @date 25 Sep, 2014
	 * @param value
	 * 	put the parameter map to queue
	 * 	signal one thread
	 */
	public int put(Map<String, String[]> value) {
		boolean error = false;
		int state = 0;
		String htable = null;
		try {
			if (value.get("json") != null || value.get("jsonr") != null) {
				String json = value.get("json") != null ? value.get("json")[0] : value.get("jsonr")[0];
				Map<String, Object> jmap = JsonUtil.fromJsonToHashMap(json);
				if (jmap != null) {
					Object data_id = jmap.get("device_id") != null ? jmap.get("device_id") : jmap.get("group_id");
					if (data_id != null) {
//						Short DeviceId = Double.valueOf(data_id.toString()).shortValue();
                        Integer DeviceId = Double.valueOf(data_id.toString()).intValue();
						htable = (value.get("htable") != null) ? value.get("htable")[0]: null;
						Short CompanyId = (short) (DeviceId / 1000);
						// 数据时间戳更新
						dataTimeUpdate(CompanyId, DeviceId, htable);
					}
				}
			} else {
				Short CompanyId = NumberUtil.Short_valueOf(value, "CompanyId");
//				Short DeviceId = NumberUtil.Short_valueOf(value, "DeviceId");
                Integer DeviceId = NumberUtil.Int_valueOf(value, "DeviceId");
				htable = (value.get("htable") != null) ? value.get("htable")[0]: null;
				// 分组数据的DeviceId小于1000，重新赋值分组数据的companyId，此数据为分组数据
				if(DeviceId < 1000 && DeviceId > 0) {
				    CompanyId = ServiceHelper.instance().getGroupInfoService().getCompanyIdByGroudId(DeviceId.shortValue());
				    String[] strCompanyId = { String.valueOf(CompanyId) };
				    value.put("CompanyId", strCompanyId);
				}
				// 数据时间戳更新
				dataTimeUpdate(CompanyId, DeviceId, htable);
			}
		} catch (Exception e2) {
			e2.printStackTrace();
			// post上来的数据本身有问题
		}
		
		try {
			// monitordatav2为容转需项目规定的数据htable
			if (htable != null && "monitordatav2".equals(htable.toLowerCase())) {
				RealtimeDataWorkerQueue.instance().put(new HashMap<String, String[]>(value));
			}
			
			lock.lock();
			if(QUEUE_FRONT_SIZE >= QUEUE_MAX_SIZE) {
				error = true;
				state = 400; // bad request
			} else {
				if(value != null && value.size() != 0) {
					queue.add(value);
					QUEUE_FRONT_SIZE++;
					monitor.signal();
					state = 200; // success
				} else {
					error = true;
					state = 400; // bad request
				}
			}
		} finally{
			lock.unlock();
			if(error) {
				LogJsonUtil.errorJsonFileRecord(getClass().getSimpleName(), "out of queue range", JsonUtil.toJson(value));
			}			
		}	
		return state;
	}
	
	/**
	 * @param CompanyId
	 * @param DeviceId
	 * @param Htable
	 * 更新数据到达的时间
	 */
	private void dataTimeUpdate(Short CompanyId, Integer DeviceId, String Htable) {
		if (Htable != null) {
			Htable = Htable.trim().toLowerCase();
			// 1. 分组数据不进行过期时间判断
			// 2. commerr degreevalue kilow数据不进行过期时间判断（原因是nbtd在频繁重启时，此3类数据是能管正常上传的）
			if(CompanyId != 0 && DeviceId >= 1000 
					&& !Htable.equals("commerr") 
					&& !Htable.equals("degreevalue")
					&& !Htable.equals("kilow")) {
				synchronized (companyIDAndTimeMaps) {
					// 第一次连接&&启动超过一分半钟：发邮件
					if(companyIDAndTimeMaps.get(CompanyId) == null && (System.currentTimeMillis() - now) > 90000) {
						 //发送同时记录发送时间
						SendEmail.sendMail(
								"COMPANY_" +
								CompanyId +
								"_START_CONNECTION", "The company start connection time is " +
								DateUtil.formatToHHMMSS(new Date()) +
								" !");
					}
					companyIDAndTimeMaps.put(CompanyId.intValue(), new Date().getTime());
				}
				synchronized (deviceIDAndTimeMaps) {
					Map<Integer, Long> temp = deviceIDAndTimeMaps.get(CompanyId);
					if(temp == null) {
						temp = new ConcurrentHashMap<Integer, Long>();
					}
					temp.put(DeviceId, new Date().getTime());
					deviceIDAndTimeMaps.put(CompanyId, temp);
				}
			}
		}
	}
	
	/**
	 * @date 25 Sep, 2014
	 * @throws InterruptedException
	 *	1. normal thread:
	 *		take value from the queue,
	 *		if the value is not null, then start to work
	 *		if the value is null, then block
	 *	2. jedis thread:
	 *		take value from the queue,
	 *		if the value is not null, then start to work
	 *		if the value is null, then
	 *			take value from the redis,
	 *			if the value is not null, then start to work
	 *			if the value is null, then block
	 */
	public Map<String, String[]> take() throws InterruptedException{
		/*String json = "{\"json\":[\"{\\\"is_group\\\":1,\\\"data_type\\\":\\\"degree_unit\\\",\\\"epi\\\":5317018.3507270003,\\\"device_id\\\":29,\\\"insert_time\\\":1513139958}\"],\"htable\":[\"json\"]}";
		Type type = new TypeToken<Map<String, String[]>>(){}.getType();
		return (Map<String, String[]>) JsonUtil.fromJsonAsGeneric(json, type);*/
		Map<String, String[]> value = null;
		try {
			lock.lock();
			value = queue.poll();
			if(value == null || value.size() == 0) {
				monitor.await();
			} else {
				QUEUE_FRONT_SIZE--;
			}
		} finally{
			lock.unlock();
		}
		
		return value;
	}
	
	public Map<String, String[]> takeNoneBlock() {
		Map<String, String[]> value = null;
		try {
			lock.lock();
			value = queue.poll();
			if(value == null || value.size() == 0) {
				return null;
			}
		} catch(Exception e) {
			e.printStackTrace();
		} finally{
			lock.unlock();
		}
		
		return value;
	}
	
	public Map<Integer, Long> cloneCompanyMap() {
		synchronized (companyIDAndTimeMaps) {
			return new HashMap<Integer, Long>(companyIDAndTimeMaps);
		}
	}
	
	public Map<Integer, Long> cloneDeviceMap(Integer companyId) {
		synchronized (deviceIDAndTimeMaps) {
			if(deviceIDAndTimeMaps.get(companyId) == null) {
				return new HashMap<Integer, Long>();
			} else {
				return new HashMap<Integer, Long>(deviceIDAndTimeMaps.get(companyId));
			}
		}
	}
	
	public boolean getStopWorking() {
		return stopWorking;
	}
	
	public void setStopWorking(boolean flag) {
		this.stopWorking = flag;
	}
	
	public Jedis getLorJedis() {
		return lorJedis;
	}
	
}
