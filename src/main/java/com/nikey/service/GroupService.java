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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.nikey.hbase.GroupHTableMapper;
import com.nikey.hbase.HbaseTablePool;
import com.nikey.util.DateUtil;
import com.nikey.util.JsonUtil;
import com.nikey.util.PropUtil;
import com.nikey.util.ScanUtil;

public class GroupService
{
	public static void main(String[] args) 
	{
        String start = "2014-12-01 00:00:00 ";  
        String end = "2015-04-01 23:00:00";  
        class Group{
        	private String groupId;
        	public String getGroupId()
			{
				return groupId;
			}
			public void setGroupId(String groupId)
			{
				this.groupId = groupId;
			}
			public String getDeviceid()
			{
				return deviceid;
			}
			public void setDeviceid(String deviceid)
			{
				this.deviceid = deviceid;
			}
			private String deviceid;
        }
        Group[] group=new Group[8];
        group[0]=new Group();
        group[0].setGroupId("4");
        group[0].setDeviceid("1024");
        
        group[1]=new Group();
        group[1].setGroupId("1002");group[1].setDeviceid("1002");
        group[2]=new Group();
        group[2].setGroupId("1003");group[2].setDeviceid("1003");
        group[3]=new Group();
        group[3].setGroupId("1006");group[3].setDeviceid("1006");
        group[4]=new Group();
        group[4].setGroupId("1009");group[4].setDeviceid("1009");
        group[5]=new Group();
        group[5].setGroupId("1019");group[5].setDeviceid("1019");
        
        group[6]=new Group();
        group[6].setGroupId("7");group[6].setDeviceid("1007,1008,1009,1010,1011,1016,1017,1023");
        group[7]=new Group();
        group[7].setGroupId("6");group[7].setDeviceid("1001,1005,1006,1012,1013,1014,1015,1016,1017,1018,1019");
        for(int i=0;i<group.length;i++){
        	try
			{
				new GroupService().scanMonitor(start, end, group[i].getDeviceid(), group[i].getGroupId());
			} catch (Exception e)
			{
				e.printStackTrace();
			}
        }
	}
    public static List<Date> findDates(Date dBegin, Date dEnd) {  
        List<Date> lDate = new ArrayList<Date>();  
        lDate.add(dBegin);  
        Calendar calBegin = Calendar.getInstance();  
        // 使用给定的 Date 设置此 Calendar 的时间    
        calBegin.setTime(dBegin);  
        Calendar calEnd = Calendar.getInstance();  
        // 使用给定的 Date 设置此 Calendar 的时间    
        calEnd.setTime(dEnd);  
        // 测试此日期是否在指定日期之后    
        while (dEnd.after(calBegin.getTime())) {  
            // 根据日历的规则，为给定的日历字段添加或减去指定的时间量    
            calBegin.add(Calendar.HOUR_OF_DAY, 1);  
            lDate.add(calBegin.getTime());  
        }  
        return lDate;  
    } 
    
	public void scanMonitor(String start,String end,String deviceidGroup,String groupId)throws ParseException, IOException {
		HTableInterface htable =  HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_monitordata_name"));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:00:00");  
        Date dBegin = sdf.parse(start);  
        Date dEnd = sdf.parse(end);  
        List<Date> lDate = findDates(dBegin, dEnd);  
    	String DeviceIdStringArr[] = deviceidGroup.split(",");
    	short CompanyId=1;
        List<Map<Long, Object>>  deviceidGroupDateList=new ArrayList<Map<Long, Object>>();
        for (Date date : lDate) {  
        	List<Map<String, Object>>  deviceidGroupList=new ArrayList<Map<String, Object>>();
        	Calendar   calendar   =  Calendar.getInstance();  
        	calendar.setTime(date); 
     	    calendar.add(calendar.MINUTE,5);
     	    Date enddate=calendar.getTime();   
     	    
        	for(int i=0;i<DeviceIdStringArr.length;i++){   	
        		Scan scan = null;
        		Map<String, Object> deviceidGroupMap=new HashMap<String, Object>();;
        		try {
        			byte[] startKey = Bytes.add(Bytes.toBytes((short)CompanyId), Bytes
        					.toBytes(Short.valueOf(DeviceIdStringArr[i])), Bytes.toBytes(date.getTime()));
        			byte[] endKey = Bytes.add(Bytes.toBytes((short)CompanyId), Bytes
        					.toBytes(Short.valueOf(DeviceIdStringArr[i])), Bytes.toBytes(enddate.getTime()));
        			scan = new Scan(startKey, endKey);
        			scan.addColumn(Bytes.toBytes("P"), new byte[]{Byte.valueOf("4")}); 
        			scan.addColumn(Bytes.toBytes("P"), new byte[]{Byte.valueOf("12")});
        			scan.addColumn(Bytes.toBytes("P"), new byte[]{Byte.valueOf("16")});
        			scan.addColumn(Bytes.toBytes("D"), new byte[]{Byte.valueOf("1")});
        			scan.addColumn(Bytes.toBytes("D"), new byte[]{Byte.valueOf("9")});
        			scan.addColumn(Bytes.toBytes("P"), new byte[]{Byte.valueOf("24")});
        		} catch (Exception e) {
        			e.printStackTrace();
        		}
        		try {
        			ResultScanner rScaner = htable.getScanner(scan);
        			int counter=0;
        			long rowtime=0;
        			Value value = new Value();
        			flag:for (Result r : rScaner) {
        				for (Cell c : r.rawCells()) {
        					if(counter<6){
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
        						String family = Bytes.toString(CellUtil.cloneFamily(c));
        						String qualifier = CellUtil.cloneQualifier(c)[0] + "";
        						String col = family + qualifier;			
        						switch (col) {
        						case "P4":
        							value.setP4(Bytes.toFloat( CellUtil.cloneValue(c)));
        							break;
        						case "P12":
        							value.setP12(Bytes.toFloat( CellUtil.cloneValue(c)));
        							break;
        						case "P16":
        							value.setP16(Bytes.toFloat( CellUtil.cloneValue(c)));
        							break;
        						case "P24":
        							value.setP24(Bytes.toFloat( CellUtil.cloneValue(c)));;
        							break;
        						case "D1":
        							value.setD1(Bytes.toDouble( CellUtil.cloneValue(c)));;
        							break;
        						case "D9":
        							value.setD9(Bytes.toDouble( CellUtil.cloneValue(c)));;
        							break;
        						}
        					} else {
        						value.setDate(DateUtil.formatToHHMMSS(rowtime));
        						break flag;
        					}
        				}
        			}
        			deviceidGroupMap.put("P0", value.getP4());
        			deviceidGroupMap.put("Q0", value.getP12());
        			deviceidGroupMap.put("S0", value.getP16());
        			deviceidGroupMap.put("Epi", value.getD1());
        			deviceidGroupMap.put("EQind", value.getD9());
        			deviceidGroupMap.put("FPdemand", value.getP24());
        				
        			Map<String, Object> singleDeviceidMap=new HashMap<String, Object>();;
        			singleDeviceidMap.put(DeviceIdStringArr[i], deviceidGroupMap);
        			deviceidGroupList.add(singleDeviceidMap);
        			
        		} catch (Exception e) {
        			e.printStackTrace();
        		}       	
        	}
			Map<Long, Object> singlMap=new HashMap<Long, Object>();;
			singlMap.put(date.getTime(), deviceidGroupList);
			deviceidGroupDateList.add(singlMap);
        }  

        GroupHTableMapper g=new GroupHTableMapper();
        g.convertParamesToPut(String.valueOf(CompanyId), groupId, deviceidGroupDateList);
		
	}
	
	class Value {
		private String date;
		private Float P4;
		private Float P12;
		private Float P16;
		private Float P24;
		private Double D1;
		private Double D9;
		public Float getP4()
		{
			return P4;
		}
		public void setP4(Float p4)
		{
			P4 = p4;
		}
		public Float getP12()
		{
			return P12;
		}
		public void setP12(Float p12)
		{
			P12 = p12;
		}
		public Float getP16()
		{
			return P16;
		}
		public void setP16(Float p16)
		{
			P16 = p16;
		}
		public Float getP24()
		{
			return P24;
		}
		public void setP24(Float p24)
		{
			P24 = p24;
		}
		public Double getD1()
		{
			return D1;
		}
		public void setD1(Double d1)
		{
			D1 = d1;
		}
		public Double getD9()
		{
			return D9;
		}
		public void setD9(Double d9)
		{
			D9 = d9;
		}
		public String getDate()
		{
			return date;
		}
		public void setDate(String date)
		{
			this.date = date;
		}
		
	}
}
