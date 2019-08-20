package com.nikey.hbase;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nikey.util.DateUtil;
import com.nikey.util.NumberUtil;
import com.nikey.util.PropUtil;
import com.nikey.util.ScanUtil;
import com.nikey.util.ServiceHelper;

public class GroupdemandHTableMapper implements HTableMapper
{
    Logger logger = LoggerFactory.getLogger(getClass());
    private ThreadLocal<HTableInterface> htables = new ThreadLocal<HTableInterface>();
    
    public static void main(String[] args) throws ParseException, IOException
    {
    	HTableInterface  htable = HbaseTablePool.instance().getHtable("group");
    	Get get = new Get(Bytes.add(Bytes.toBytes((short) 5),
                Bytes.toBytes((short) 20),
                Bytes.toBytes(1511765220000l)));
        try {
        	Result r = htable.get(get);
        	for (Cell c : r.rawCells()) {
                String family = Bytes.toString(CellUtil.cloneFamily(c));
                String qualifier = CellUtil.cloneQualifier(c)[0] + "";
                String col = family + qualifier;   
                switch (col) {
                case "A4":
                    long  demandTime=Bytes.toLong( CellUtil.cloneValue(c));
                    System.out.println(demandTime);
                    break;
                case "A5":
                    float  demand=Bytes.toFloat( CellUtil.cloneValue(c));
                    System.out.println(demand);
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Override
    public List<Put> convertParameterMapToPut(Map<String, String[]> request)
    {
    	short CompanyId = 0;
		short DeviceId = 0;
		long DemandTime = 0l;
		try {
			CompanyId = NumberUtil.Short_valueOf(request, "CompanyId");
			DeviceId = NumberUtil.Short_valueOf(request, "DeviceId");
			DemandTime = NumberUtil.Long_valueOf(request, "DemandTime");
			DemandTime = DemandTime * 1000l; // 放大为毫秒值
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
                Bytes.toBytes(DemandTime)));

        float Demand = NumberUtil.Float_valueOf(request, "Demand");
        byte[] FuQualifier = { 4, 5 };
        put.add(Bytes.toBytes("A"), new byte[]{FuQualifier[0]}, 0,
                Bytes.toBytes(DemandTime));
        if (Demand < 0) {
        	put.add(Bytes.toBytes("A"), new byte[]{FuQualifier[1]}, 4096l,
                    Bytes.toBytes(0f));
        } else {
        	put.add(Bytes.toBytes("A"), new byte[]{FuQualifier[1]}, 0,
                    Bytes.toBytes(Demand));
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
            String path = folder + File.separator + "backup" + File.separator + "groupdemand" + File.separator 
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
            String path = folder + File.separator + "backup2" + File.separator + "groupdemand" + File.separator 
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
