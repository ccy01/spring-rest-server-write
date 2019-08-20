package com.nikey.hbase;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nikey.util.DateUtil;
import com.nikey.util.NumberUtil;
import com.nikey.util.PropUtil;
import com.nikey.util.ServiceHelper;

/**
 * @author jayzee
 * @date 28 Sep, 2014
 *	GrouppTable
 */
public class GroupHTableMapper implements HTableMapper{
	
	/**
	 * slfj
	 */
	Logger logger = LoggerFactory.getLogger(getClass());
	
	private ThreadLocal<HTableInterface> htables = new ThreadLocal<HTableInterface>();

	@Override
	public List<Put> convertParameterMapToPut(Map<String, String[]> request) {
		short CompanyId = 0;
		short DeviceId = 0;
		long InsertTime = 0l;
		try {
			CompanyId = NumberUtil.Short_valueOf(request, "CompanyId");
			DeviceId = NumberUtil.Short_valueOf(request, "DeviceId");
			InsertTime = NumberUtil.Long_valueOf(request, "InsertTime");
			InsertTime = InsertTime * 1000l; // 放大为毫秒值
		} catch (Exception e) {
			// 数据畸变，返回null
			e.printStackTrace();
			return null;
		}
		
		if (CompanyId == 0) {
        	CompanyId = ServiceHelper.instance().getGroupInfoService().getCompanyIdByGroudId(DeviceId);
        }
        if (CompanyId == 0) {
        	logger.error("GroupId {} no match companyId", DeviceId);
        	return null;
        }
		
		Put put = new Put(Bytes.add(Bytes.toBytes(CompanyId),
				Bytes.toBytes(DeviceId),
				Bytes.toBytes(InsertTime)));
		
		float P0 = Float.valueOf(request.get("P0")[0]);
		double Epi = Double.valueOf(request.get("Epi")[0]);
		
		byte[] FaQualifier = { 1, 2 };
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[0]}, 0,
				Bytes.toBytes(P0));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[1]}, 0,
				Bytes.toBytes(Epi));
//		terminalSettingsBackup(DeviceId, "point2:CompanyId:" + CompanyId + ",GroupId:" + DeviceId + ",InsertTime:" + DateUtil.formatToHHMMSS(InsertTime));	
		List<Put> puts = new ArrayList<Put>();
		puts.add(put);
		return puts;
	}

	public void convertParamesToPut(String companyId,String groupId,List<Map<Long, Object>>  deviceidGroupDateList) {
		int count=0;
		short CompanyId = Short.valueOf(companyId);
		short GroupId = Short.valueOf(groupId);
        for(int i=0;i<deviceidGroupDateList.size();i++){
        	Map<Long, Object>  dateMap=deviceidGroupDateList.get(i);
        	for (Long datetime : dateMap.keySet()) {        		  
        		List<Map<String, Object>> deviceidAllList=(List<Map<String, Object>>) dateMap.get(datetime);
        		Put put = new Put(Bytes.add(Bytes.toBytes(CompanyId),Bytes.toBytes(GroupId),Bytes.toBytes(datetime)));
        		boolean flag=true;
        		Double EQind=0.0;
        		for(int j=0;j<deviceidAllList.size();j++){
        			Map<String, Object>  devicesingleMap=deviceidAllList.get(j);
        			for (String deviceid : devicesingleMap.keySet()) { 
        				Map<String, Object>  dataMap=(Map<String, Object>) devicesingleMap.get(deviceid);
        				// FIXME 数据类型错误
        				if(dataMap.get("EQind")!=null){
        					EQind+=(double)dataMap.get("EQind");
        				}
        				else{
        					flag=false;
        				}
        			}
        		}
        		if(flag){
        			put.add(Bytes.toBytes("A"), new byte[]{Byte.valueOf("7")}, 0,Bytes.toBytes(EQind));    			
        		}
        		else{
        			put.add(Bytes.toBytes("A"), new byte[]{Byte.valueOf("7")}, 4096,Bytes.toBytes((double)0));    
        		}
        		flag=true;
        		float P0=0;
        		for(int j=0;j<deviceidAllList.size();j++){
        			Map<String, Object>  devicesingleMap=deviceidAllList.get(j);
        			for (String deviceid : devicesingleMap.keySet()) { 
        				Map<String, Object>  dataMap=(Map<String, Object>) devicesingleMap.get(deviceid);
        				if(dataMap.get("P0")!=null){
        					P0+=(float)dataMap.get("P0");
        				}
        				else{
        					flag=false;
        				}
        			}
        		}
        		if(flag){
        			put.add(Bytes.toBytes("A"), new byte[]{Byte.valueOf("1")}, 0,Bytes.toBytes(P0));    			
        		}
        		else{
        			put.add(Bytes.toBytes("A"), new byte[]{Byte.valueOf("1")}, 4096,Bytes.toBytes((float)0));    
        		}
        		flag=true;
        		float Q0=0;
        		for(int j=0;j<deviceidAllList.size();j++){
        			Map<String, Object>  devicesingleMap=deviceidAllList.get(j);
        			for (String deviceid : devicesingleMap.keySet()) { 
        				Map<String, Object>  dataMap=(Map<String, Object>) devicesingleMap.get(deviceid);
        				if(dataMap.get("Q0")!=null){
        					Q0+=(float)dataMap.get("Q0");
        				}
        				else{
        					flag=false;
        				}
        			}
        		}
        		if(flag){
        			put.add(Bytes.toBytes("A"), new byte[]{Byte.valueOf("2")}, 0,Bytes.toBytes(Q0));    			
        		}
        		else{
        			put.add(Bytes.toBytes("A"), new byte[]{Byte.valueOf("1")}, 4096,Bytes.toBytes((float)0));    
        		}
        		flag=true;
        		float S0=0;
        		for(int j=0;j<deviceidAllList.size();j++){
        			Map<String, Object>  devicesingleMap=deviceidAllList.get(j);
        			for (String deviceid : devicesingleMap.keySet()) { 
        				Map<String, Object>  dataMap=(Map<String, Object>) devicesingleMap.get(deviceid);
        				if(dataMap.get("S0")!=null){
        					S0+=(float)dataMap.get("S0");
        				}
        				else{
        					flag=false;
        				}
        			}
        		}
        		if(flag){
        			put.add(Bytes.toBytes("A"), new byte[]{Byte.valueOf("3")}, 0,Bytes.toBytes(S0));    			
        		}
        		else{
        			put.add(Bytes.toBytes("A"), new byte[]{Byte.valueOf("3")}, 4096,Bytes.toBytes((float)0));    
        		}
        		flag=true;
        		Double Epi=0.0;
        		for(int j=0;j<deviceidAllList.size();j++){
        			Map<String, Object>  devicesingleMap=deviceidAllList.get(j);
        			for (String deviceid : devicesingleMap.keySet()) { 
        				Map<String, Object>  dataMap=(Map<String, Object>) devicesingleMap.get(deviceid);
        				if(dataMap.get("Epi")!=null){
        					Epi+=(Double)dataMap.get("Epi");
        				}
        				else{
        					flag=false;
        				}
        			}
        		}
        		if(flag){
        			put.add(Bytes.toBytes("A"),new byte[]{Byte.valueOf("6")}, 0,Bytes.toBytes(Epi));    			
        		}
        		else{
        			put.add(Bytes.toBytes("A"), new byte[]{Byte.valueOf("6")}, 4096,Bytes.toBytes((double)0));    
        		}
        		flag=true;
        		float FPdemand=0;
        		for(int j=0;j<deviceidAllList.size();j++){
        			Map<String, Object>  devicesingleMap=deviceidAllList.get(j);
        			for (String deviceid : devicesingleMap.keySet()) { 
        				Map<String, Object>  dataMap=(Map<String, Object>) devicesingleMap.get(deviceid);
        				if(dataMap.get("FPdemand")!=null){
        					FPdemand+=(float)dataMap.get("FPdemand");
        				}
        				else{
        					flag=false;
        				}
        			}
        		}
        		if(flag){
        			put.add(Bytes.toBytes("A"), new byte[]{Byte.valueOf("5")}, 0,Bytes.toBytes(FPdemand));    			
        		}
        		else{
        			put.add(Bytes.toBytes("A"), new byte[]{Byte.valueOf("5")}, 4096,Bytes.toBytes((float)0));    
        		}
        		put.add(Bytes.toBytes("A"), new byte[]{Byte.valueOf("4")}, 0,Bytes.toBytes(datetime));    		
        		count++;
        		try {
        			HTableInterface htable =  HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_group_name"));
        			htable.put(put);
        			htable.flushCommits();// write to hbase immediately
        			htable.close();
        			System.out.println(datetime);
        			System.out.println(CompanyId);
        			System.out.println(GroupId);
        			
        		} catch (Exception ingnore) {
        			ingnore.printStackTrace();
        		}  
        		
        	} 
        }
	}
	
	@Override
	public boolean put(final List<Put> put) {		
		if(htables.get() == null) {
			try {
				htables.set(HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_group_name")));
			} catch (IOException e) {
				e.printStackTrace();
				return false;
			}
		}
		final HTableInterface htable = htables.get();		
		
		try {
			//final long start = System.currentTimeMillis();
			
			htable.put(put);
			htable.flushCommits();// write to hbase immediately
			htable.close();
			//logger.info("put to hbase success..." + (System.currentTimeMillis() - start));
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public ResultScanner getResultScannerWithParameterMap(Scan scan) {
		HTableInterface htable;
		try {
			htable = HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_group_name"));
			try {
				long start = System.currentTimeMillis();
				ResultScanner rScaner = htable.getScanner(scan);
				logger.info("get to hbase success..." + (System.currentTimeMillis() - start));
				return rScaner;
			} catch (IOException e) {
				return null;
			}
		} catch (IOException e) {
			return null;
		}
	}
	
	/**
	 * backup the terminal settings to local file
	 * @param deviceid
	 * @param backupstr
	 */
	private void terminalSettingsBackup(Short deviceid, String backupstr) {
		File directory = new File("");//设定为当前文件夹
		try{
			String folder = directory.getAbsolutePath();
			String path = folder + File.separator + "backup" + File.separator + "group" + File.separator 
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
