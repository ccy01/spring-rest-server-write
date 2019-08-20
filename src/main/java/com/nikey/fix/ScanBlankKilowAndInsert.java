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
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.nikey.util.DataFormatUtil;
import com.nikey.util.PropUtil;
import com.nikey.hbase.HbaseTablePool;
import com.nikey.util.DateUtil;
import com.nikey.util.NumberUtil;
import com.nikey.util.ScanUtil;

public class ScanBlankKilowAndInsert
{

    public static void main(String[] args)
    {
        if(args.length==6){
            String tableNames=args[0];
            String columnFamilys=args[1];
            String sDeviceId=args[2];
            String eDeviceId=args[3];
            String offsetTime=args[4];
            String isInsert=args[5];
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
                scanTable(tableNames,CompanyId, StartDeviceId, EndDeviceId, startTime, endTime, htable, columnFamilys,offsetTime,isInsert);
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
    static public void scanTable(String tableNames,short CompanyId, short StartDeviceId, short EndDeviceId, long startTime, long endTime, HTableInterface htable, String columnFamily,String offsetTime,String isInsert){
        if(StartDeviceId!=0&&EndDeviceId!=0){
            for(short DeviceID=StartDeviceId;DeviceID<=EndDeviceId;DeviceID++){
                StringBuffer sb=new StringBuffer();
                if(tableNames.equals("monitordata")&&columnFamily.equals("D")){
                    List<Put> puts = new ArrayList<Put>();
                    long offset=60*60*1000;
                    for(long indextime=startTime;indextime<=endTime;indextime+=offset){
                        Get get=new Get(Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(DeviceID),Bytes.toBytes(indextime)));
                        get.addColumn(Bytes.toBytes("D"), new byte[]{Byte.valueOf("1")}); 
                        try
                        {
                            Result r=htable.get(get);
                            int length=r.rawCells().length;
                            if(length==0){
                                byte epiQualifier = ScanUtil
                                        .getQualifierByteByTableNameAndQualifierName(
                                                PropUtil.getString("htable_mapper_monitordata"), "Epi");
                                byte epoQualifier = ScanUtil
                                        .getQualifierByteByTableNameAndQualifierName(
                                                PropUtil.getString("htable_mapper_monitordata"),
                                                "Epo");
                                byte eQindQualifier = ScanUtil
                                        .getQualifierByteByTableNameAndQualifierName(
                                                PropUtil.getString("htable_mapper_monitordata"),
                                                "EQind");

                                byte eQcapQualifier = ScanUtil
                                        .getQualifierByteByTableNameAndQualifierName(
                                                PropUtil.getString("htable_mapper_monitordata"),
                                                "EQcap");
                                long longoffsetTime=Long.valueOf(offsetTime)*60*1000;
                                byte[] startKey=Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(DeviceID),Bytes.toBytes(indextime));
                                byte[] endKey=Bytes.add(Bytes.toBytes(CompanyId),Bytes.toBytes(DeviceID),Bytes.toBytes(indextime-longoffsetTime));
                                Scan scan=new Scan(startKey,endKey);
                                scan.setTimeStamp(0l);
                                scan.setFilter(new PageFilter(1));
                                scan.setReversed(true);
                                scan.addColumn(Bytes.toBytes(columnFamily), new byte[]{Byte.valueOf("1")});
                                scan.addColumn(Bytes.toBytes(columnFamily), new byte[]{Byte.valueOf("8")});
                                scan.addColumn(Bytes.toBytes(columnFamily), new byte[]{Byte.valueOf("9")});
                                scan.addColumn(Bytes.toBytes(columnFamily), new byte[]{Byte.valueOf("10")});
                                ResultScanner scanner = htable.getScanner(scan);
                                Object[] thisEpi = null, lastEpi = null;
                                Object[] thisEp0 = null, lastEpo = null;
                                Object[] thisEqind = null, lastEqind = null;
                                Object[] thisEqcap = null, lastEqcap = null;
                                
                                long thisTime=0 ,lastTime=0;
                                if(scanner != null) {
                                    try {
                                        for (Cell cell : scanner.next().rawCells()) {
                                            
                                            byte qualifier = CellUtil.cloneQualifier(cell)[0];
                                            double value = Bytes.toDouble(CellUtil.cloneValue(cell));
                                            // 格式化保留两位小数
                                            value = DataFormatUtil.formatDoubleTwoBits(value);
                                            long time = ScanUtil.getTimeByCell(cell);
                                            if(thisTime==0){
                                                thisTime=time;
                                            }
                                            if (qualifier == epiQualifier) {
                                                thisEpi = new Object[] { time, value };
                                            } else if (qualifier == epoQualifier) {
                                                thisEp0 = new Object[] { time, value };
                                            } else if (qualifier == eQindQualifier) {
                                                thisEqind = new Object[] { time, value };
                                            } else if (qualifier == eQcapQualifier) {
                                                thisEqcap = new Object[] { time, value };
                                            }
                                        }
                                    } catch (Exception e) {
                                        //
                                    } finally {
                                        scanner.close();
                                    }
                                }
                                byte[] startKey2=Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(DeviceID),Bytes.toBytes(indextime));
                                byte[] endKey2=Bytes.add(Bytes.toBytes(CompanyId),Bytes.toBytes(DeviceID),Bytes.toBytes(indextime+longoffsetTime));
                                Scan scan2=new Scan(startKey2,endKey2);
                                scan2.setTimeStamp(0l);
                                scan2.setFilter(new PageFilter(1));
                                scan2.addColumn(Bytes.toBytes(columnFamily), new byte[]{Byte.valueOf("1")});
                                scan2.addColumn(Bytes.toBytes(columnFamily), new byte[]{Byte.valueOf("8")});
                                scan2.addColumn(Bytes.toBytes(columnFamily), new byte[]{Byte.valueOf("9")});
                                scan2.addColumn(Bytes.toBytes(columnFamily), new byte[]{Byte.valueOf("10")});
                                ResultScanner scanner2 = htable.getScanner(scan2);
                                if(scanner2 != null) {
                                    try {
                                        for (Cell cell : scanner2.next().rawCells()) {
                                            byte qualifier = CellUtil.cloneQualifier(cell)[0];
                                            double value = Bytes.toDouble(CellUtil.cloneValue(cell));
                                            // 格式化保留两位小数
                                            value = DataFormatUtil.formatDoubleTwoBits(value);
                                            long time = ScanUtil.getTimeByCell(cell);
                                            if(lastTime==0){
                                                lastTime=time;
                                            }
                                            if (qualifier == epiQualifier) {
                                                lastEpi = new Object[] { time, value };
                                            } else if (qualifier == epoQualifier) {
                                                lastEpo = new Object[] { time, value };
                                            } else if (qualifier == eQindQualifier) {
                                                lastEqind = new Object[] { time, value };
                                            } else if (qualifier == eQcapQualifier) {
                                                lastEqcap = new Object[] { time, value };
                                            }
                                        }
                                    } catch (Exception e) {
                                        //
                                    } finally {
                                        scanner.close();
                                    }
                                }
                                sb.append("first time is :"+DateUtil.formatToHHMMSS(thisTime)+"  second time is :"+DateUtil.formatToHHMMSS(lastTime)+"\n");
                                if(thisTime!=0 && lastTime!=0 &&thisTime!=lastTime){
                                    if(isInsert.equals("true")){
                                        Put put = new Put(Bytes.add(Bytes.toBytes(CompanyId),
                                                Bytes.toBytes(DeviceID),
                                                Bytes.toBytes(indextime)));
                                        
                                        
                                        double Epi = getValue((long)lastEpi[0],(double)lastEpi[1],(long)thisEpi[0],(double)thisEpi[1],indextime);
                                        double Epo = getValue((long)lastEpo[0],(double)lastEpo[1],(long)thisEp0[0],(double)thisEp0[1],indextime);
                                        double EQind = getValue((long)lastEqind[0],(double)lastEqind[1],(long)thisEqind[0],(double)thisEqind[1],indextime);
                                        double EQcap = getValue((long)lastEqcap[0],(double)lastEqcap[1],(long)thisEqcap[0],(double)thisEqcap[1],indextime);
                                        
                                        sb.append("indextime :"+DateUtil.formatToHHMMSS(indextime)+"\n");
                                        sb.append("first Epi is :"+(double)thisEpi[1]+"  second Epi is :"+(double)lastEpi[1]+"\n");
                                        sb.append("insert Epi is :"+Epi+"\n");
                                        sb.append("first Epo is :"+(double)thisEp0[1]+"  second Epo is :"+(double)lastEpo[1]+"\n");
                                        sb.append("insert Epo is :"+Epo+"\n");
                                        sb.append("first EQind is :"+(double)thisEqind[1]+"  second EQind is :"+(double)lastEqind[1]+"\n");
                                        sb.append("insert EQind is :"+EQind+"\n");
                                        sb.append("first EQcap is :"+(double)thisEqcap[1]+"  second EQcap is :"+(double)lastEqcap[1]+"\n");
                                        sb.append("insert EQcap is :"+EQcap+"\n");
                                        byte[] FdQualifier = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
                                                15, 16, 17, 18, 19, 20, 21};
                                        put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[0]}, 0,
                                                Bytes.toBytes(Epi));
                                        put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[7]}, 0,
                                                Bytes.toBytes(Epo));
                                        put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[8]}, 0,
                                                Bytes.toBytes(EQind));
                                        put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[9]}, 0,
                                                Bytes.toBytes(EQcap));
                                        puts.add(put);
                                    }
                                }
                            }
                        } catch (IOException e)
                        {
                            e.printStackTrace();
                        }
                    }
/*                    if(puts.size()!=0){
                        try {
                            htable.put(puts);
                            htable.flushCommits();// write to hbase immediately
                            htable.close();
                        } catch (Exception ingnore) {
                        } 
                    }*/
                }
                terminalSettingsBackup2(tableNames,columnFamily,DeviceID,sb.toString());
            }
        }
    }
    static public void terminalSettingsBackup2(String tableName,String columnFamily,Short deviceid, String backupstr) {
        File directory = new File("");//设定为当前文件夹
        try{
            String folder = directory.getAbsolutePath();
            String path = folder + File.separator + "backupD" + File.separator + tableName +File.separator + columnFamily+ File.separator 
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
    static public double getValue(long time1,double value1,long time2,double value2,long insertime){
        double k=(value2-value1)/(time2-time1);
        double b=value1-k*time1;
        double insertValue=insertime*k+b;
        return DataFormatUtil.formatDoubleTwoBits(insertValue);
    }
    

}
