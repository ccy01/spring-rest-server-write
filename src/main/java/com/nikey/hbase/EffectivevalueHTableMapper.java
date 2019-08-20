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

public class EffectivevalueHTableMapper implements HTableMapper
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
		
        short EventType= NumberUtil.Short_valueOf(request, "EventType");
        
        Put put = new Put(Bytes.add(Bytes.toBytes(CompanyId),
                Bytes.toBytes(DeviceId),
                Bytes.toBytes(InsertTime)));

        float Value0 = NumberUtil.Float_valueOf(request, "Value0");
        float Value1 = NumberUtil.Float_valueOf(request, "Value1");
        float Value2 = NumberUtil.Float_valueOf(request, "Value2");
        
        switch(EventType)
        {
        case 11001:
        case 11002:
        case 11005:
        case 21001:
            if(Value0!=-10000&&Value1!=-10000&&Value2!=-10000){
                put.add(Bytes.toBytes("U"), new byte[]{(byte)1}, 0,
                        Bytes.toBytes(Value0));
                put.add(Bytes.toBytes("U"), new byte[]{(byte)2}, 0,
                        Bytes.toBytes(Value1));
                put.add(Bytes.toBytes("U"), new byte[]{(byte)3}, 0,
                        Bytes.toBytes(Value2));
            }
            break;
        case 11003:
        case 21002:
            if(Value0!=-10000&&Value1!=-10000&&Value2!=-10000){
                put.add(Bytes.toBytes("I"), new byte[]{(byte)1}, 0,
                        Bytes.toBytes(Value0));
                put.add(Bytes.toBytes("I"), new byte[]{(byte)2}, 0,
                        Bytes.toBytes(Value1));
                put.add(Bytes.toBytes("I"), new byte[]{(byte)3}, 0,
                        Bytes.toBytes(Value2));
            }
            break;
        case 11004:
        case 21003:
            if(Value0!=-10000){
                String istransfomer=ServiceHelper.instance().getTransFormerInfoService().isTransFormerById(DeviceId);
                if(istransfomer!=null){
                    if(istransfomer.equals("0")){
                        put.add(Bytes.toBytes("P"), new byte[]{(byte)4}, 0,
                                Bytes.toBytes(Value0));
                    }
                    else{
                        put.add(Bytes.toBytes("P"), new byte[]{(byte)16}, 0,
                                Bytes.toBytes(Value0));
                    }
                }
            }
            break;
        case 11006:
            if(Value0!=-10000){
                put.add(Bytes.toBytes("I"), new byte[]{(byte)19}, 0,
                        Bytes.toBytes(Value0));
            }
            break;
        case 22002:
            if(Value0!=-10000&&Value1!=-10000&&Value2!=-10000){
                put.add(Bytes.toBytes("U"), new byte[]{(byte)18}, 0,
                        Bytes.toBytes(Value0));
                put.add(Bytes.toBytes("U"), new byte[]{(byte)19}, 0,
                        Bytes.toBytes(Value1));
                put.add(Bytes.toBytes("U"), new byte[]{(byte)20}, 0,
                        Bytes.toBytes(Value2));
            }
            break;
        case 22003:
            if(Value0!=-10000&&Value1!=-10000&&Value2!=-10000){
                put.add(Bytes.toBytes("U"), new byte[]{(byte)21}, 0,
                        Bytes.toBytes(Value0));
                put.add(Bytes.toBytes("U"), new byte[]{(byte)22}, 0,
                        Bytes.toBytes(Value1));
                put.add(Bytes.toBytes("U"), new byte[]{(byte)23}, 0,
                        Bytes.toBytes(Value2));
            }
            break;
        }
        StringBuffer sb=new StringBuffer();
        sb.append("point:CompanyId:" + CompanyId +",EventType:"+EventType+ ",DeviceId:" + DeviceId + ",InsertTime:" + DateUtil.formatToHHMMSS(InsertTime)+"\n");
        sb.append("Value0:"+Value0+"\n");
        sb.append("Value1"+Value1+"\n");
        sb.append("Value2"+Value2+"\n");
//        terminalSettingsBackup(DeviceId, "point:CompanyId:" + CompanyId + ",DeviceId:" + DeviceId +",EventType:"+EventType+ ",InsertTime:" + DateUtil.formatToHHMMSS(InsertTime)
//                                +",Value0:"+Value0+",Value1:"+Value1+",Value2:"+Value2);
//        terminalSettingsBackup2(DeviceId, sb.toString());
        List<Put> puts = new ArrayList<Put>();
        puts.add(put);
        return puts;
    }

    @Override
    public boolean put(List<Put> put)
    {       
        if(htables.get() == null) {
            try {
                htables.set(HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_monitordata_name")));
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
            htable = HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_monitordata_name"));
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
            String path = folder + File.separator + "backup" + File.separator + "effectivevalue" + File.separator 
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
            String path = folder + File.separator + "backup2" + File.separator + "effectivevalue" + File.separator 
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