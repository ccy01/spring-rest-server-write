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

public class GrouppowerHTableMapper implements HTableMapper
{
    
    Logger logger = LoggerFactory.getLogger(getClass());
    private ThreadLocal<HTableInterface> htables = new ThreadLocal<HTableInterface>();
    
    @Override
    public List<Put> convertParameterMapToPut(Map<String, String[]> request)
    {
    	short CompanyId = 0;
		short DeviceId = 0;
		long InsertTime = 0l;
		try {
			CompanyId = NumberUtil.Short_valueOf(request, "CompanyId");
			DeviceId = NumberUtil.Short_valueOf(request, "DeviceId");
			InsertTime = NumberUtil.Long_valueOf(request, "HappenTime");
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

        float P0 = NumberUtil.Float_valueOf(request, "P0");
        float Q0 = NumberUtil.Float_valueOf(request, "Q0");
        float S0 = NumberUtil.Float_valueOf(request, "S0");
        double isTrue = Double.valueOf(request.get("IsTrue")[0]);
        
        byte[] FuQualifier = { 1, 2, 3 ,12};
        if (isTrue < 0) {
        	float value = 0f;
        	long timestamp = 0l;
            put.add(Bytes.toBytes("A"), new byte[]{FuQualifier[0]}, timestamp,
                    Bytes.toBytes(value));
            put.add(Bytes.toBytes("A"), new byte[]{FuQualifier[1]}, timestamp,
                    Bytes.toBytes(value));
            put.add(Bytes.toBytes("A"), new byte[]{FuQualifier[2]}, timestamp,
                    Bytes.toBytes(value));
            put.add(Bytes.toBytes("A"), new byte[]{FuQualifier[3]}, timestamp,
                    Bytes.toBytes(value));
        } else {
            put.add(Bytes.toBytes("A"), new byte[]{FuQualifier[0]}, 0,
                    Bytes.toBytes(P0));
            put.add(Bytes.toBytes("A"), new byte[]{FuQualifier[1]}, 0,
                    Bytes.toBytes(Q0));
            put.add(Bytes.toBytes("A"), new byte[]{FuQualifier[2]}, 0,
                    Bytes.toBytes(S0));
            float PowerFactor = 0;
            if (NumberUtil.isZero(S0) || NumberUtil.isZero(P0)) {
            	PowerFactor = 1;
            } else {
            	PowerFactor = P0 / S0;
            }
            put.add(Bytes.toBytes("A"), new byte[]{FuQualifier[3]}, 0,
                    Bytes.toBytes(PowerFactor));
        }
        List<Put> puts = new ArrayList<Put>();
        puts.add(put);
        return puts;
    }

    @Override
    public boolean put(List<Put> put)
    {       
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
    public ResultScanner getResultScannerWithParameterMap(Scan scan)
    {
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
    
    private void terminalSettingsBackup(Short deviceid, String backupstr) {
        File directory = new File("");//设定为当前文件夹
        try{
            String folder = directory.getAbsolutePath();
            String path = folder + File.separator + "backup" + File.separator + "grouppower" + File.separator 
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
    
    private void terminalSettingsBackup2(Short deviceid, String backupstr) {
        File directory = new File("");//设定为当前文件夹
        try{
            String folder = directory.getAbsolutePath();
            String path = folder + File.separator + "backup2" + File.separator + "grouppower" + File.separator 
                    + deviceid + ".txt";
            
            File file = new File(path);
            if(!file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }
            if(!file.exists()) { 
                file.createNewFile();
            }
            if(file.exists()) {
                FileWriter writer = new FileWriter(file);
                writer.write(backupstr + "\n");
                writer.flush();
                writer.close();
                
            }
        }catch(Exception e){}
    }
    

}
