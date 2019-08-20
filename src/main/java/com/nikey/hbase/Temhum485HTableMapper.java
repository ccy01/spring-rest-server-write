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

public class Temhum485HTableMapper implements HTableMapper
{
    Logger logger = LoggerFactory.getLogger(getClass());
    private ThreadLocal<HTableInterface> htables=new ThreadLocal<HTableInterface>();
    
    public static void main(String[] args) throws ParseException
    {
        short  CompanyId=5;
        short DeviceId=5901;
        String starDate="2016-06-22 00:00:00";
        String endDate="2016-06-23 01:00:00";
        SimpleDateFormat sim=new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date sDate=sim.parse(starDate);
        Date eDate=sim.parse(endDate);
        long stime=sDate.getTime();
        long etime=eDate.getTime();
        
        try
        {
            HTableInterface htable=HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_temperature_name"));
            byte[] startkey=Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(DeviceId),Bytes.toBytes(stime));
            byte[] ednKey=Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(DeviceId),Bytes.toBytes(etime));
            Scan scan=new Scan(startkey, ednKey);
            byte[] quailfer={4,5,6};
            scan.addColumn(Bytes.toBytes("A"), new byte[]{quailfer[0]});
            scan.addColumn(Bytes.toBytes("A"), new byte[]{quailfer[1]});
            scan.addColumn(Bytes.toBytes("A"), new byte[]{quailfer[2]});
    //        System.out.println("--------------------DeviceId:  "+DeviceId);
            scan.setTimeRange(0, Long.MAX_VALUE);
            ResultScanner rScan=htable.getScanner(scan);
            for(Result r:rScan){
                for(Cell c:r.rawCells()){
                    String family=Bytes.toString(CellUtil.cloneFamily(c));
                    String column=String.valueOf(CellUtil.cloneQualifier(c)[0]);
                    String col=family+column;
                    long time=ScanUtil.getTimeByCell(c);
                    Date d=new Date(time);
                    long timestemp=c.getTimestamp();
                    byte b=(byte)timestemp;
                    byte b4=(byte) (b&(0x0f));
                    byte b8=(byte) (b&(0xf0));
                    if(timestemp>>12==1)
                    {
                        continue;//断点数据
                    }
          //          System.out.println("--------------time"+d);
           //         System.out.println("-----------timestemp"+timestemp);
                    switch(col){
                    case "A4":
                        float dianfang=Bytes.toFloat(CellUtil.cloneValue(c));
            //            System.out.println("---------------dianfang:"+dianfang);;
                        System.out.println("-----------timestemp"+b8);
                        break;
                    case "A5":
                        float diangui=Bytes.toFloat(CellUtil.cloneValue(c));
     //                   System.out.println("---------------diangui"+diangui);
      //                  System.out.println("-----------timestemp"+timestemp);
                        break;
                    case "A6":
                        float Humidity=Bytes.toFloat(CellUtil.cloneValue(c));
        //                System.out.println("------------Humidity"+Humidity);;
        //                System.out.println("-----------timestemp"+timestemp);
                        break;
                    }
                }
            }
        } catch (IOException e)
        {
            e.printStackTrace();
        }
        
        
    }
    @Override
    public List<Put> convertParameterMapToPut(Map<String, String[]> request)
    {
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
		
        int type=ServiceHelper.instance().getKlm4134Service().getRelationMap(DeviceId);
        if(type!=0){
            List<Put> puts=new ArrayList<Put>();
            Put put=new Put(Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(DeviceId),Bytes.toBytes(InsertTime)));
            float Humidity=NumberUtil.Float_valueOf(request, "Humidity");
            float Temperature=NumberUtil.Float_valueOf(request, "Temperature");
            //type为1是高压电房，2是低压电房
            if(type==1||type==2){
                put.add(Bytes.toBytes("A"), new byte[]{Byte.valueOf("4")}, 0, Bytes.toBytes(Temperature));
            }
            else{
                put.add(Bytes.toBytes("A"), new byte[]{Byte.valueOf("5")}, 0, Bytes.toBytes(Temperature));
            }
            put.add(Bytes.toBytes("A"), new byte[]{Byte.valueOf("6")}, 0, Bytes.toBytes(Humidity));
            StringBuffer sb=new StringBuffer();
            sb.append("point:CompanyId:" + CompanyId + ",TemperatureId:" + DeviceId + ",InsertTime:" + DateUtil.formatToHHMMSS(InsertTime)+"\n");
            sb.append("Humidity:"+Humidity+"\n");
            sb.append("Temperature:"+Temperature+"\n");
//            terminalSettingsBackup(DeviceId, "point:CompanyId:" + CompanyId + ",TemperatureId:" + DeviceId + ",InsertTime:" + DateUtil.formatToHHMMSS(InsertTime));
//            terminalSettingsBackup2(DeviceId, sb.toString());
            puts.add(put);
            return puts;
        }
        else{
            return null;
        }
        
    }

    @Override
    public boolean put(List<Put> put)
    {
        if(htables.get()==null){
            try
            {
                htables.set(HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_temperature_name")));
            } catch (IOException e)
            {
                e.printStackTrace();
                return false;
            }
        }
        HTableInterface htable=htables.get();
        try
        {
            htable.put(put);
            htable.flushCommits();
            htable.close();
            return true;
        } catch (IOException e)
        {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public ResultScanner getResultScannerWithParameterMap(Scan scan)
    {
        HTableInterface htable;
        try
        {
            htable=HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_temperature_name"));
        } catch (IOException e)
        {
            return null;
        }
        try
        {
            long start = System.currentTimeMillis();
            ResultScanner rScaner = htable.getScanner(scan);
            logger.info("get to hbase success..." + (System.currentTimeMillis() - start));
            return rScaner;
        } catch (IOException e)
        {
            e.printStackTrace();
            return null;
        }
    }
    private void terminalSettingsBackup(Short deviceid, String backupstr) {
        File directory = new File("");//设定为当前文件夹
        try{
            String folder = directory.getAbsolutePath();
            String path = folder + File.separator + "backup" + File.separator + "temhun485" + File.separator 
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
            String path = folder + File.separator + "backup2" + File.separator + "temhun485" + File.separator 
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
