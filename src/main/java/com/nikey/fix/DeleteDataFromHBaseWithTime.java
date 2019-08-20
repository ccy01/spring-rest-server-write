package com.nikey.fix;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.nikey.hbase.HbaseTablePool;
import com.nikey.util.ScanUtil;


/**  
*   
* 项目名称：SpringRestServerWrite  
* 类名称：setUp   
* 创建人：dyf  
* 修改时间：2016年6月29日 下午8:05:17  
* 修改备注：方法用于删除hbase某个时间段内表格的数据 
* @version   
*   
*/
public class DeleteDataFromHBaseWithTime
{
    public static void main(String[] args)
    {
        SimpleDateFormat sim=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String startStr="2016-05-01 00:00:00";
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
        String[] tableNames={"monitordata","degreevalue"};
        String[] columnFamilys={"D","A"};
        for(int i=0;i<tableNames.length;i++){
            List<Delete> deletes=new ArrayList<Delete>();
            try
            {
                short CompanyId=5;
                short StartDeviceId=5001;
                short EndDeviceId=5010;
                HTableInterface htable =  HbaseTablePool.instance().getHtable(tableNames[i]);
                deletes=getDeleteList(CompanyId, StartDeviceId, EndDeviceId, startTime, endTime, htable, columnFamilys[i]);
                if(deletes!=null&&deletes.size()!=0){
                    System.out.println("start delete "+tableNames[i]+" data number is:"+deletes.size());
                    htable.delete(deletes);
                    htable.flushCommits();
                    System.out.println(tableNames[i]+"delete data success!");
                }
                else{
                    System.out.println(tableNames[i]+"has no data");
                }
            } catch (IOException e)
            {
                e.printStackTrace();
            }
        }
        
    }
    
    static public List<Delete>getDeleteList(short CompanyId, short StartDeviceId, short EndDeviceId, long startTime, long endTime, HTableInterface htable, String columnFamily){
        if(StartDeviceId!=0&&EndDeviceId!=0){
            List<Delete> deletes=new ArrayList<Delete>();
            for(short DeviceID=StartDeviceId;DeviceID<=EndDeviceId;DeviceID++){
                byte[] startKey=Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(DeviceID),Bytes.toBytes(startTime));
                byte[] endKey=Bytes.add(Bytes.toBytes(CompanyId),Bytes.toBytes(DeviceID),Bytes.toBytes(endTime));
                Scan scan=new Scan(startKey,endKey);
                scan.addColumn(Bytes.toBytes(columnFamily), new byte[]{Byte.valueOf("1")});
                try {
                    ResultScanner rScaner = htable.getScanner(scan);
                    for (Result r : rScaner) {
                        long InsertTime = 0;
                        for (Cell cell : r.rawCells()) {
                            InsertTime = ScanUtil.getTimeByCell(cell);
                            if(InsertTime <= endTime) {
                                Delete delete = new Delete(Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(DeviceID), Bytes.toBytes(InsertTime)));
                                deletes.add(delete);
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            return deletes;
        }
        return null;
    }
}
