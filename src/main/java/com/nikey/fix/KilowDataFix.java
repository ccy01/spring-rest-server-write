package com.nikey.fix;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
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

import com.nikey.hbase.HbaseTablePool;
import com.nikey.util.DataFormatUtil;
import com.nikey.util.DateUtil;
import com.nikey.util.PropUtil;
import com.nikey.util.ScanUtil;

/**  
*   
* 项目名称：SpringRestServerWrite  
* 类名称：KilowDataFix  
* 类描述：  
* 创建人：dyf  
* 创建时间：2016年7月11日 上午9:46:33  
* 修改人：dyf  
* 修改时间：2016年7月11日 上午9:46:33  
* 修改备注：修改峰平谷数据，根据起始时间和正确的峰平谷时间段修改
* @version   
*   
*/
public class KilowDataFix
{

    public static void main(String[] args)
    {
        String tableNames="monitordata";
        String columnFamilys="D";
        String sDeviceId="5001";
        String eDeviceId="5010";
        short CompanyId=5;
        short StartDeviceId=Short.valueOf(sDeviceId);
        short EndDeviceId=Short.valueOf(eDeviceId);
        System.out.println("start scan "+tableNames+"  columnFamilys:"+columnFamilys+"  DeviceId:"+sDeviceId);
        SimpleDateFormat sim=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String startStr="2016-06-24 14:00:00";
        String endStr="2016-06-17 00:00:00";
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
    static public void scanTable(String tableNames,short CompanyId, short StartDeviceId, short EndDeviceId, long startTime, long endTime, HTableInterface htable, String columnFamily){
        //峰平谷时间段，pvf[0]表示 00:00:00的峰平谷值
        int[] pvf={3,3,3,3,3,3,3,3,2,1,1,1,2,2,2,2,2,2,2,1,1,1,2,2};
        if(StartDeviceId!=0&&EndDeviceId!=0){
            byte epiQualifier = ScanUtil
                    .getQualifierByteByTableNameAndQualifierName(
                            PropUtil.getString("htable_mapper_monitordata"), "Epi");
            byte peakEpiQualifier = ScanUtil
                    .getQualifierByteByTableNameAndQualifierName(
                            PropUtil.getString("htable_mapper_monitordata"),
                            "PeakEpi");
            byte flatEpiQualifier = ScanUtil
                    .getQualifierByteByTableNameAndQualifierName(
                            PropUtil.getString("htable_mapper_monitordata"),
                            "FlatEpi");

            byte valleyEpiQualifier = ScanUtil
                    .getQualifierByteByTableNameAndQualifierName(
                            PropUtil.getString("htable_mapper_monitordata"),
                            "ValleyEpi");
            for(short DeviceID=StartDeviceId;DeviceID<=EndDeviceId;DeviceID++){
                StringBuffer sb=new StringBuffer();

                List<Put> puts = new ArrayList<Put>();
                long offset=60*60*1000;
                //firstData[0]存放epi值，firstData[1]存放PeakEpi值，firstData[2]存放FlatEpi值，firstData[3]存放ValleyEpi值
                //secondData[0]存放epi值，secondData[1]存放PeakEpi值，secondData[2]存放FlatEpi值，secondData[3]存放ValleyEpi值
                double[] firstData=null; double[] secondData=null;boolean isfirst=true;
                for(long indextime=startTime;indextime>=endTime;indextime-=offset){
                    Get get=new Get(Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(DeviceID),Bytes.toBytes(indextime)));
                    get.addColumn(Bytes.toBytes("D"), new byte[]{Byte.valueOf("1")}); 
                    get.addColumn(Bytes.toBytes("D"), new byte[]{Byte.valueOf("12")}); 
                    get.addColumn(Bytes.toBytes("D"), new byte[]{Byte.valueOf("13")}); 
                    get.addColumn(Bytes.toBytes("D"), new byte[]{Byte.valueOf("14")}); 
                    Object[] thisEpi = null;
                    Object[] thisPeakEpi = null;
                    Object[] thisFlatEpi = null;
                    Object[] thisValleyEpi = null;
                    try
                    {
                        Result r=htable.get(get);
                        for (Cell cell : r.rawCells()) {
                            
                            byte qualifier = CellUtil.cloneQualifier(cell)[0];
                            double value = Bytes.toDouble(CellUtil.cloneValue(cell));
                            // 格式化保留两位小数
                            value = DataFormatUtil.formatDoubleTwoBits(value);
                            long time = ScanUtil.getTimeByCell(cell);
                            if (qualifier == epiQualifier) {
                                thisEpi = new Object[] { time, value };
                            } else if (qualifier == peakEpiQualifier) {
                                thisPeakEpi = new Object[] { time, value };
                            } else if (qualifier == flatEpiQualifier) {
                                thisFlatEpi = new Object[] { time, value };
                            } else if (qualifier == valleyEpiQualifier) {
                                thisValleyEpi = new Object[] { time, value };
                            }
                        }
                        if(firstData==null){
                            firstData=new double[4];
                            firstData[0]=(double) thisEpi[1];
                            firstData[1]=(double) thisPeakEpi[1];
                            firstData[2]=(double) thisFlatEpi[1];
                            firstData[3]=(double) thisValleyEpi[1];
                        }
                        else{
                            secondData=new double[4];
                            secondData[0]=(double) thisEpi[1];
                            secondData[1]=firstData[1];
                            secondData[2]=firstData[2];
                            secondData[3]=firstData[3];
                        }
                        if(firstData!=null&&secondData!=null){
                            Calendar calendar = Calendar.getInstance();
                            Date date =new Date(indextime);
                            calendar.setTime(date);
                            int hour=calendar.get(Calendar.HOUR_OF_DAY);
                            double aveepi=firstData[0]-secondData[0];
                            //根据时间修改对应的峰平谷值
                            secondData[pvf[hour]]=firstData[pvf[hour]]-aveepi;
                            Put put = new Put(Bytes.add(Bytes.toBytes(CompanyId),
                                    Bytes.toBytes(DeviceID),
                                    Bytes.toBytes(indextime)));
                            
                            
                            double Epi = secondData[0];
                            double PeakEpi = secondData[1];
                            double FlatEpi = secondData[2];
                            double ValleyEpi =secondData[3];
                            
                            sb.append("indextime :"+DateUtil.formatToHHMMSS(indextime)+"\n");
                            sb.append("first Epi is :"+firstData[0]+"  second Epi is :"+secondData[0]+"\n");
                            sb.append("first PeakEpi is :"+firstData[1]+"  second PeakEpi is :"+secondData[1]+"\n");
                            sb.append("first FlatEpi is :"+firstData[2]+"  second FlatEpi is :"+secondData[2]+"\n");
                            sb.append("first ValleyEpi is :"+firstData[3]+"  second ValleyEpi is :"+secondData[3]+"\n");
                            byte[] FdQualifier = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
                                    15, 16, 17, 18, 19, 20, 21};
                            put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[0]}, 0,
                                    Bytes.toBytes(Epi));
                            put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[11]}, 0,
                                    Bytes.toBytes(PeakEpi));
                            put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[12]}, 0,
                                    Bytes.toBytes(FlatEpi));
                            put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[13]}, 0,
                                    Bytes.toBytes(ValleyEpi));
                            puts.add(put);
                            firstData=secondData;
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
