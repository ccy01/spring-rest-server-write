package com.nikey.thread;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nikey.util.DateUtil;
import com.nikey.util.HostNameUtil;
import com.nikey.util.PropUtil;
import com.nikey.util.SendEmail;

/**
 * 负责超时判断和邮件通告
 * 
 * @author jayzee
 */
public class TimerWorker extends Thread {
	
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final Map<Integer, Map<String,Object>> sendEmailTimeMap = new ConcurrentHashMap<Integer, Map<String,Object>>();
	
	@Override
	public void run() {		
		if (HostNameUtil.isAliyun()) {
			logger.info("start up time judge thread worker ...");
			work();
		}
	}
	
	private void work() {
		while(! Thread.currentThread().isInterrupted() && ! WorkQueue.instance().getStopWorking()) {
			try {
				timeJudge();
				Thread.sleep(30000);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	private synchronized void timeJudge() {
		Map<Integer, Long> companyMap = WorkQueue.instance().cloneCompanyMap();
		logger.info("companyMap size is : " + companyMap.size());
		connectionTimeOut(companyMap, true);
	}

	private void connectionTimeOut(Map<Integer, Long> companyOrDeviceMap, boolean isCompany) {
		try {
			Set<Integer> companyOrDeviceKeySet = companyOrDeviceMap.keySet();
			Long nowLong = new Date().getTime();
			Long sendMailInterval = PropUtil.getLong("send_mail_interval");
			Long connectionJudgeInterval = PropUtil.getLong("connection_judge_interval");
			int send_mail_count=PropUtil.getInt("send_mail_count");
			
			for(Integer companyOrDevice : companyOrDeviceKeySet) {
			    //判断当是客户9时，将断开时间延长到30分钟
				if((isCompany && companyOrDevice == 9)||(companyOrDevice / 1000 == 9)){
				    connectionJudgeInterval = connectionJudgeInterval * 3;
				}
				// 数据一旦断开10分钟则触发判断
				if((nowLong - companyOrDeviceMap.get(companyOrDevice)) >= connectionJudgeInterval) {
					// 每隔5分钟发一份邮件
					Map<String, Object> sendEmailItem = sendEmailTimeMap.get(companyOrDevice);
					int count = 0;
					if(sendEmailItem == null) { // 没发过邮件
						count = 1;
						sendEmailItem = new HashMap<String,Object>();
						sendEmailItem.put("count", count);
					} else if(Integer.valueOf(sendEmailItem.get("count").toString()) < send_mail_count &&
							(nowLong - Long.valueOf(sendEmailItem.get("time").toString())) >= sendMailInterval) { // 发过邮件了：计数未满 && 邮件时间达到间隔
						count = Integer.valueOf(sendEmailItem.get("count").toString()) + 1;
						sendEmailItem.put("count", count);
					}
					if(count != 0) {
						// 发送同时记录发送时间
					    if (companyOrDevice / 1000 != 9) { // 过滤广交会的DEVICE报警，因为数目实在是太多了
	                        SendEmail.sendMail(
                                (isCompany ? "COMPANY_" : "DEVICE_") +
                                 companyOrDevice +
                                "_CONNECTION_TIMEOUT_" + count, "The last connection time is " +
                                        DateUtil.formatToHHMMSS(companyOrDeviceMap.get(companyOrDevice)) +
                                ", pls check the communication !");
					    }
						sendEmailItem.put("time", nowLong);
						sendEmailTimeMap.put(companyOrDevice, sendEmailItem);
					}
				} else {
					if(isCompany) {
						Map<Integer, Long> deviceMap = WorkQueue.instance().cloneDeviceMap(companyOrDevice);
						logger.info("deviceMap size is : " + deviceMap.size());
						if(sendEmailTimeMap.get(companyOrDevice) != null) {
							// 公司之前处于数据中断，现在恢复了，但可能只是其中某一个监测点恢复而已，因此这里要防止其他监测点数据还未到而导致的邮件报警
							Set<Integer> deviceSet = deviceMap.keySet(); // 拿到该公司全部监测点编号
							for(Integer deviceID : deviceSet) {
								Map<String,Object> temp = sendEmailTimeMap.get(deviceID);
								if(temp == null) {
									temp = new HashMap<String,Object>();
								}
								temp.put("count", 1);
								temp.put("time", nowLong);
								sendEmailTimeMap.put(deviceID, temp);
							}
						} else {
							// 公司之前没有处于数据中断状态，检测该公司的所有监测点连接情况是否正常
							connectionTimeOut(deviceMap, false);
						}
					}
					if(sendEmailTimeMap.get(companyOrDevice) != null) {
				        sendEmailTimeMap.remove(companyOrDevice);
				    }
				}
				//当是客户9时，将时间还原
                if((isCompany && companyOrDevice == 9)||(companyOrDevice / 1000 == 9)){
                    connectionJudgeInterval = connectionJudgeInterval / 3;
                }
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
