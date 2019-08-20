package com.nikey.fix;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.nikey.hbase.HbaseTablePool;
import com.nikey.util.DateUtil;
import com.nikey.util.ScanUtil;

/**  
*   
* 项目名称：SpringRestServerWrite  
* 类名称：ScanBlankDataTime  
* 类描述：  
* 创建人：dyf  
* 创建时间：2016年7月9日 下午2:34:45  
* 修改人：dyf  
* 修改时间：2016年7月9日 下午2:34:45  
* 修改备注：  
* @version   
*   
*/
public class ScanBlankDataTime
{

    public static void main(String[] args)
    {
        if(args.length==4){
            String tableNames=args[0];
            String columnFamilys=args[1];
            String sDeviceId=args[2];
            String eDeviceId=args[3];
            short CompanyId=5;
            short StartDeviceId=Short.valueOf(sDeviceId);
            short EndDeviceId=Short.valueOf(eDeviceId);
            System.out.println("start scan "+tableNames+"  columnFamilys:"+columnFamilys+"  DeviceId:"+sDeviceId);
            SimpleDateFormat sim=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String startStr="2016-06-17 00:00:00";
            String endStr="2016-07-09 00:00:00";
            Date startDate=null;
            Date endDate=null;
            try
            {
                startDate=sim.parse(startStr);
                endDate = sim.parse(endStr);
            } catch (ParseException e1)
            {
                e1.printStackTrace();
            }
            long startTime=startDate.getTime();
            long endTime=endDate.getTime();
            try
            {
                HTableInterface htable =  HbaseTablePool.instance().getHtable(tableNames);
                scanTable(tableNames,CompanyId, StartDeviceId, EndDeviceId, startTime, endTime, htable, columnFamilys);
                System.out.println("end scan "+tableNames+" is success");
            } catch (IOException e)
            {
                e.printStackTrace();
            }
        }
        else{
            System.out.println("请输入正确的参数！");
        }
    }
    static public void scanTable(String tableNames,short CompanyId, short StartDeviceId, short EndDeviceId, long startTime, long endTime, HTableInterface htable, String columnFamily){
        if(StartDeviceId!=0&&EndDeviceId!=0){
            for(short DeviceID=StartDeviceId;DeviceID<=EndDeviceId;DeviceID++){
                StringBuffer sb=new StringBuffer();
                if(tableNames.equals("monitordata")&&columnFamily.equals("D")){
                    long offset=60*60*1000;
                    for(long indextime=startTime;indextime<=endTime;indextime+=offset){
                        Get get=new Get(Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(DeviceID),Bytes.toBytes(indextime)));
                        get.addColumn(Bytes.toBytes("D"), new byte[]{Byte.valueOf("1")}); 
                        try
                        {
                            Result r=htable.get(get);
                            int length=r.rawCells().length;
                            System.out.println("indextime is "+indextime+"  and result length is "+length);
                            if(length==0){
                                sb.append("insert_time = "+indextime/1000+"\n");
                            }
                        } catch (IOException e)
                        {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }
                }
                else{
                    byte[] startKey=Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(DeviceID),Bytes.toBytes(startTime));
                    byte[] endKey=Bytes.add(Bytes.toBytes(CompanyId),Bytes.toBytes(DeviceID),Bytes.toBytes(endTime));
                    Scan scan=new Scan(startKey,endKey);
                    if(tableNames.equals("temperature")){
                        scan.addColumn(Bytes.toBytes(columnFamily), new byte[]{Byte.valueOf("6")});
                    }
                    else{
                        scan.addColumn(Bytes.toBytes(columnFamily), new byte[]{Byte.valueOf("1")});
                    }
                    try {
                        Long firstGoodTime = 0l, secondGoodTime = 0l;
                        long sub = 600000l + 60000; // 600s + 60s
                        ResultScanner rScaner = htable.getScanner(scan);
                        for (Result r : rScaner) {
                            for (Cell cell : r.rawCells()) {
                                long InsertTime = ScanUtil.getTimeByCell(cell);
                                if(cell.getTimestamp() != 4096l) {
                                    if(firstGoodTime == 0) {
                                        firstGoodTime = InsertTime;
                                        System.out.println("DeviceID is "+DeviceID+"firstGoodTime is "+firstGoodTime);
                                    } else {
                                        if(secondGoodTime == 0) {
                                            secondGoodTime = InsertTime;
                                        } else {
                                            firstGoodTime = secondGoodTime;
                                            secondGoodTime = InsertTime;
                                        }
                                        // 两个正常点相距时间超过10min
                                        if((secondGoodTime - firstGoodTime) > sub) {
                                            sb.append("开始时间 ： "+DateUtil.formatToHHMMSS(firstGoodTime)+"  结束时间 ： "+DateUtil.formatToHHMMSS(secondGoodTime)+"  间隔时间 ："+((secondGoodTime - firstGoodTime)/1000)+"\n");
                                        } 
                                    }
                                } 
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                terminalSettingsBackup2(tableNames,columnFamily,DeviceID,sb.toString());
            }
        }
    }
    static public void terminalSettingsBackup2(String tableName,String columnFamily,Short deviceid, String backupstr) {
        File directory = new File("");//设定为当前文件夹
        try{
            String folder = directory.getAbsolutePath();
            String path = folder + File.separator + "backup2" + File.separator + tableName +File.separator + columnFamily+ File.separator 
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
