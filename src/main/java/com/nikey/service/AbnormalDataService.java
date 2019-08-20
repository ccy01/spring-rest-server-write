package com.nikey.service;

import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.nikey.bean.Abnormaldata;
import com.nikey.mapper.AbnormalDataMapper;
import com.nikey.util.HostNameUtil;
import com.nikey.util.JobControllUtil;
import com.nikey.util.JsonUtil;
import com.nikey.util.LogJsonUtil;
import com.nikey.util.ServiceHelper;

@Service
public class AbnormalDataService {
	
	public AbnormalDataService() {
		ServiceHelper.instance().setAbnormalDataService(this);
	}
	
	/**
	 * slfj
	 */
	Logger logger = LoggerFactory.getLogger(getClass());
	
	@Autowired
	private AbnormalDataMapper abnormalDataMapper;
	
	public synchronized void handleCommerr(final Abnormaldata abnormaldata, final long endtime) {
		Callable<String> task = new Callable<String>() {

			@Override
			public String call() throws Exception {
				try {
					int result = 0;
					// 实时
					if(endtime == 0) {
						// 查询历史表是否存在该key的数据（防止结束的数据先到）
						result = abnormalDataMapper.selectCommerrHistoryCount(abnormaldata);
						// 脏数据
						if(result > 0) {
							// 丢弃 不做任何处理
							LogJsonUtil.errorJsonFileRecord(getClass().getSimpleName(), "dirty commerr", JsonUtil.toJson(abnormaldata));
						}
						else {
							// 插入实时通信异常数据
							result = abnormalDataMapper.insertCommerrReal(abnormaldata);
						}
					} 
					// 历史
					else {
						// 删除通信异常实时数据  -- 考虑一些奇异值没有来得及删除 --
						result = abnormalDataMapper.deleteCommerrReal(abnormaldata);
						// 删除通信异常历史数据  -- 考虑一些奇异值没有来得及删除 --
						result = abnormalDataMapper.deleteCommerrHistory(abnormaldata);
						// 新增通信异常历史数据
						result = abnormalDataMapper.insertCommerrHistory(abnormaldata);
					}
				} catch (Exception e) {
					LogJsonUtil.errorJsonFileRecord(getClass().getSimpleName(), e.getMessage(), JsonUtil.toJson(abnormaldata));
				}
				return "done";
			}
			
		};
		JobControllUtil.submitJob(task, abnormaldata, getClass().getSimpleName());
	}
	
    private Map<Integer, Map<String, Object>> cache = new HashMap<Integer, Map<String, Object>>();
    
    public  Map<String, Object> getRelationMap(int id) {
        synchronized (cache) {
            if(cache.get(id) != null) {
                return cache.get(id);
            }
        }
        try {
            Map<String, Object> result = new HashMap<String, Object>();
            result = abnormalDataMapper.getEventDataById(id);
            if(result != null) {
                synchronized (cache) {
                    cache.put(id, result);
                }
            }
            return result;
        } catch (Exception e) {
            logger.error("Real temperatrue device {} can't get relation in MySQL !", id);
            return null;
        }
    }
    
    public synchronized void insertCraftEpi(int deviceId,String changeTime,double epi,int operationType, int isGroup) {
    	abnormalDataMapper.insertCraftEpi(deviceId, changeTime, epi,operationType, isGroup);
	}
	
	public synchronized  void  handleAlarmdata(final Abnormaldata abnormaldata, final long endtime,final long starttime) {
		if (! HostNameUtil.isAliyun()) {
			logger.info("consuming alarmdata x3");
		}
		
		Callable<String> task = new Callable<String>() {
			@Override
			public String call() throws Exception {
				try {
					int result = 0;
                    int deviceId=abnormaldata.getDeviceId();
                    //特殊逻辑：监测点第二位为数字9的是温度监测点，如5901为电房监测点;不是数字9的是电参量监测点，如5001监测点
                    int subDeviceId=deviceId%1000;
                    /*int companyId=deviceId/1000;
                    boolean isSend=false;
					int type=abnormaldata.getEventType();
                    long nowTime=new Date().getTime();
                    String eventType=null;
                    String deviceName=null;
                    Map<String, Object> EventTyperesult=null;
                    Map<String, Object> Monitordataresult=null;
                    switch (type)
                    {
                    case 11001:
                    case 11002:
                    case 11003:
                    case 11004:
                    case 11005:
                    case 11006:
                    case 11010:
                    case 11011:
                    case 11012:
                        isSend=true;
                        break;
                    }
                    if(type==11012){
                        if(deviceId==5005||deviceId==5006||deviceId==5007||deviceId==5008){
                            isSend=false;
                        }
                    }
					if(isSend==true){
	                    EventTyperesult=ServiceHelper.instance().getAbnormalDataService().getRelationMap(type);
	                    if(EventTyperesult!=null){
	                        eventType=(String) EventTyperesult.get("alarm_event_type_name");
	                    }
	                    Monitordataresult=ServiceHelper.instance().getMonitordataService().getRelationMap((short) abnormaldata.getDeviceId());
	                    if(Monitordataresult!=null){
	                        deviceName=(String) Monitordataresult.get("monitory_point_Name");
	                    }
					}*/
					// 实时
					if(endtime == 0) {
					    if(subDeviceId>900){
                            //温度监测点
                            // 查询温度历史表是否存在该key的数据（防止结束的数据先到）
                            result = abnormalDataMapper.selectTemperatreAlarmdataHistoryCount(abnormaldata);
                            // 脏数据
                            if(result > 0) {
                                // 丢弃 不做任何处理
                            }
                            else {
//                                terminalSettingsBackupTemperature(abnormaldata.getDeviceId(),"point:EventType:" + abnormaldata.getEventType() + ",InsertTime:" + abnormaldata.getStartTime()+",EndTime:"+endtime+",Currentvalue:"+abnormaldata.getCurrenctValue());
                                // 插入实时越限数据
                                result = abnormalDataMapper.insertTemperatureAlarmdataReal(abnormaldata);
                            }
					    }
					    else{
	                        // 查询历史表是否存在该key的数据（防止结束的数据先到）
	                        result = abnormalDataMapper.selectAlarmdataHistoryCount(abnormaldata);
	                        
	                        // 脏数据
	                        if(result > 0) {
	                            // 丢弃 不做任何处理
	                        }
	                        else {
	                            /*if((nowTime-starttime)>3600000){
	                                isSend=false;
	                            }
	                            if(isSend){
	                                if(eventType!=null&&deviceName!=null){
	                                    //【报警】[1变压器低压侧]监测点于[08-10 8:58:03]发生[欠电压]
	                                    if(companyId==5){
	                                        SendEmail.sendMailToCompany("【报警】"+"["+deviceName+"]"+"监测点于["+DateUtil.formatToHHMMSS(starttime)+"]发生["+eventType+"]", "越限值："+abnormaldata.getCurrenctValue()+"\n发生时间："+DateUtil.formatToHHMMSS(starttime));
	                                    }
	                                }
	                            }*/
//	                            terminalSettingsBackup(abnormaldata.getDeviceId(),"point:EventType:" + abnormaldata.getEventType() + ",InsertTime:" + abnormaldata.getStartTime()+",EndTime:"+endtime+",Currentvalue:"+abnormaldata.getCurrenctValue());
	                            // 插入实时越限数据
	                            result = abnormalDataMapper.insertAlarmdataReal(abnormaldata);
	                        } 
					    }
					} 
					// 历史
					else {
                        if(subDeviceId>900){
                            //温度监测点
                            // 删除越限实时数据  -- 考虑一些奇异值没有来得及删除 --
                            result = abnormalDataMapper.deleteTemperatureAlarmdataReal(abnormaldata);
                            if (HostNameUtil.isAliyun()) {
                            	// 删除越限历史数据  -- 考虑一些奇异值没有来得及删除 --
                                result = abnormalDataMapper.deleteTemperatureAlarmdataHistory(abnormaldata);
                                // 新增越限历史数据
                                result = abnormalDataMapper.insertTemperatureAlarmdataHistory(abnormaldata);
                            }
                        }
                        else{
                            /*if((nowTime-endtime)>3600000){
                                isSend=false;
                            }
                            if(isSend){
                                if(eventType!=null&&deviceName!=null){
                                    if(companyId==5){
                                        SendEmail.sendMailToCompany("["+deviceName+"]监测点于["+DateUtil.formatToHHMMSS(endtime)+"]结束["+eventType+"]", "越限值："+abnormaldata.getCurrenctValue()+"\n发生时间："+DateUtil.formatToHHMMSS(starttime)
                                        +"\n结束时间："+DateUtil.formatToHHMMSS(endtime)+"\n持续时间："+abnormaldata.getDuration()+"秒");
                                    }
                                }
                            }*/
                            // 删除越限实时数据  -- 考虑一些奇异值没有来得及删除 --
                            result = abnormalDataMapper.deleteAlarmdataReal(abnormaldata);
                            if (HostNameUtil.isAliyun()) {
                            	// 删除越限历史数据  -- 考虑一些奇异值没有来得及删除 --
                                result = abnormalDataMapper.deleteAlarmdataHistory(abnormaldata);
                                // 新增越限历史数据
                                result = abnormalDataMapper.insertAlarmdataHistory(abnormaldata); 
                            }
                            
                            // 当事件类型为电压骤升、骤降时，不应存在历史数据而不插入实时表
	                        if(abnormaldata.getEventType() == 21006
	                        		|| abnormaldata.getEventType() == 21007
	                        		|| abnormaldata.getEventType() == 22011
	                        		|| abnormaldata.getEventType() == 22012) {
	                        	result = abnormalDataMapper.insertAlarmdataReal(abnormaldata);
	                        }
                        }
					}
				} catch (Exception e) {
					LogJsonUtil.errorJsonFileRecord(getClass().getSimpleName(), e.getMessage(), JsonUtil.toJson(abnormaldata));
				}
				return "done";
			}
			
		};
		JobControllUtil.submitJob(task, abnormaldata, getClass().getSimpleName());
	}
	@SuppressWarnings("unused")
    private void terminalSettingsBackup(int deviceid, String backupstr) {
        File directory = new File("");//设定为当前文件夹
        try{
            String folder = directory.getAbsolutePath();
            String path = folder + File.separator + "backuprealtime" + File.separator + "alarmdata" + File.separator 
                    + deviceid + ".txt";
            
            File file = new File(path);
            if(!file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }
            if(!file.exists()) { 
                file.createNewFile();
            }
            if(file.exists()) {
                FileWriter writer = new FileWriter(file, true);
                writer.write(backupstr + "\n");
                writer.flush();
                writer.close();
                
            }
        }catch(Exception e){}
    }
    @SuppressWarnings("unused")
    private void terminalSettingsBackupTemperature(int deviceid, String backupstr) {
        File directory = new File("");//设定为当前文件夹
        try{
            String folder = directory.getAbsolutePath();
            String path = folder + File.separator + "backuprealtime" + File.separator + "alarmdataTemperature" + File.separator 
                    + deviceid + ".txt";
            
            File file = new File(path);
            if(!file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }
            if(!file.exists()) { 
                file.createNewFile();
            }
            if(file.exists()) {
                FileWriter writer = new FileWriter(file, true);
                writer.write(backupstr + "\n");
                writer.flush();
                writer.close();
                
            }
        }catch(Exception e){}
    }
}
