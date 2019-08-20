package com.nikey.service;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
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

import com.nikey.hbase.GroupHTableMapper;
import com.nikey.hbase.HbaseTablePool;
import com.nikey.service.GroupService.Value;
import com.nikey.util.DateUtil;
import com.nikey.util.PropUtil;
import com.nikey.util.ScanUtil;

public class U0Service
{
	public static void main(String[] args) throws ParseException, IOException
	{
    	short CompanyId=1;
        String start = "2014-10-01 00:00:00 ";  
        String end = "2015-04-01 23:00:00";  
        String deviceidGroup="1001,1002,1003,1004,1005,1006,1007,1008,1009,1010,1011,1012,1013,1014,1015,1016,1017,1018,1019,1020,1021,1022,1023,1024";
		new U0Service().scanMonitor(start, end, deviceidGroup, CompanyId);
	}
	
	public void scanMonitor(String start,String end,String deviceidGroup,short CompanyId)throws ParseException, IOException {
		HTableInterface htable =  HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_monitordata_name"));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:00:00");  
        Date dBegin = sdf.parse(start);  
        Date dEnd = sdf.parse(end);  
    	String DeviceIdStringArr[] = deviceidGroup.split(",");     	    
        	for(int i=0;i<DeviceIdStringArr.length;i++){   	
        		Scan scan = null;
        		try {
        			byte[] startKey = Bytes.add(Bytes.toBytes((short)CompanyId), Bytes
        					.toBytes(Short.valueOf(DeviceIdStringArr[i])), Bytes.toBytes(dBegin.getTime()));
        			byte[] endKey = Bytes.add(Bytes.toBytes((short)CompanyId), Bytes
        					.toBytes(Short.valueOf(DeviceIdStringArr[i])), Bytes.toBytes(dEnd.getTime()));
        			scan = new Scan(startKey, endKey);
        			scan.addColumn(Bytes.toBytes("U"), new byte[]{Byte.valueOf("4")}); 
        			scan.addColumn(Bytes.toBytes("U"), new byte[]{Byte.valueOf("15")});
        			scan.addColumn(Bytes.toBytes("U"), new byte[]{Byte.valueOf("16")});
        			scan.setTimeRange(0,Long.MAX_VALUE);
        		} catch (Exception e) {
        			e.printStackTrace();
        		}
        		try {
        			ResultScanner rScaner = htable.getScanner(scan);
        			long rowtime=0;
         			int counter=0;
         			float U0=0;
         			float ZBPHu=0;
         			float JBoUp=0;
         			long timestamp=0;
        			for (Result r : rScaner) {
        				for (Cell c : r.rawCells()) {
        					if(counter<3){
        						if(counter==0){
        							counter++;
        							rowtime=ScanUtil.getTimeByCell(c);
        						}
        						else{
        							if(ScanUtil.getTimeByCell(c)==rowtime){
        								counter++;
        							}
        							else{
        								rowtime=ScanUtil.getTimeByCell(c);
        								counter=1;
        							}
        						}
							    rowtime=ScanUtil.getTimeByCell(c);
        						String family = Bytes.toString(CellUtil.cloneFamily(c));
        						String qualifier = CellUtil.cloneQualifier(c)[0] + "";
        						String col = family + qualifier;			
        						switch (col) {
        						case "U4":
        							U0=Bytes.toFloat( CellUtil.cloneValue(c));
        							break;
        						case "U15":
        							ZBPHu=Bytes.toFloat( CellUtil.cloneValue(c));
        							timestamp=c.getTimestamp();
        							break;
        						case "U16":
        							JBoUp=Bytes.toFloat( CellUtil.cloneValue(c));
        							break;
        						}
        					
        						if(counter==3){
        							U0=ZBPHu*JBoUp;
        							U0=U0/100;
        							new U0Service().putUintoMonitordata(DeviceIdStringArr[i], CompanyId, U0, rowtime,timestamp);
        							System.out.println(DeviceIdStringArr[i]+"  :  "+DateUtil.formatToHHMMSS(rowtime)+" : "+timestamp+"  : U0 :"+U0);
        							U0=0;
        							ZBPHu=0;
        							JBoUp=0;
        							timestamp=0;
        							counter=0;
        						}
        					}
        					} 
        				}
        		} catch (Exception e) {
        			e.printStackTrace();
        		}       	
        	}
        }  
	public void putUintoMonitordata(String deviceid,short CompanyId,float U0,long rowtime, long timestamp) {
		Put put = new Put(Bytes.add(Bytes.toBytes(CompanyId),Bytes.toBytes(Short.valueOf(deviceid)),Bytes.toBytes(rowtime)));
 	    put.add(Bytes.toBytes("U"), new byte[]{Byte.valueOf("4")}, timestamp,Bytes.toBytes(U0));    		
		try {
			HTableInterface htable =  HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_monitordata_name"));
			htable.put(put);
			htable.flushCommits();// write to hbase immediately
			htable.close();
			
		} catch (Exception ingnore) {
			ingnore.printStackTrace();
		}
	}
}
