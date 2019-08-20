package com.nikey.test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.nikey.hbase.HbaseTablePool;
import com.nikey.util.ScanUtil;

public class AlarmdataTest {
	
	public static void main(String[] args) throws ParseException, IOException {
		AlarmdataTest key = new AlarmdataTest();
		short companyId = Short.valueOf(args[0]);
		short deviceId = Short.valueOf(args[1]);
		key.scanKey(companyId, deviceId, args[2], args[3]);
	}

	public void scanKey(short CompanyId, short DeviceId, String startTime, String endTime) throws ParseException, IOException {
		HTableInterface htable =  HbaseTablePool.instance().getHtable("alarmdata");
				
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		Scan scan = null;
		try {
			byte[] startKey = Bytes.add(Bytes.toBytes((short)CompanyId), Bytes
					.toBytes((short)DeviceId), Bytes.toBytes(format.parse(startTime).getTime()));
			byte[] endKey = Bytes.add(Bytes.toBytes((short)CompanyId), Bytes
					.toBytes((short)DeviceId), Bytes.toBytes(format.parse(endTime).getTime()));
			 scan = new Scan(startKey, endKey);
		} catch (Exception e) {
			byte[] startKey = Bytes.add(Bytes.toBytes((short)CompanyId), Bytes
					.toBytes((short)DeviceId), Bytes.toBytes(Long.valueOf(startTime)));
			byte[] endKey = Bytes.add(Bytes.toBytes((short)CompanyId), Bytes
					.toBytes((short)DeviceId), Bytes.toBytes(Long.valueOf(endTime)));
			scan = new Scan(startKey, endKey);
		}
		
		try {
			ResultScanner rScaner = htable.getScanner(scan);
			for (Result r : rScaner) {
				long EndTime = 0;
				long Duration = 0;
				float Currentvalue = 0;
				byte Flag = 0;
				short EventType = 0;
				long InsertTime = 0;
				for (Cell cell : r.rawCells()) {
					// 匹配qualifier:考虑值为空的情况
					byte qulifier = CellUtil.cloneQualifier(cell)[0];
					// if you want to get <CompanyId> or <DeviceId>, then use ScanUtil.getDeviceIdByCell(cell) or ScanUtil.getCompanyIdByCell(cell)
					InsertTime = ScanUtil.getTimeByCell(cell);
					EventType = ScanUtil.getEventTypeByCell(cell);
					
					if(qulifier == 1) {
						EndTime = Bytes.toLong(CellUtil.cloneValue(cell));
					} else if(qulifier == 2) {
						Duration = Bytes.toLong(CellUtil.cloneValue(cell));
					} else if(qulifier == 3) {
						Currentvalue = Bytes.toFloat(CellUtil.cloneValue(cell));
					} else if(qulifier == 4) {
						Flag = CellUtil.cloneValue(cell)[0];
					} else if(qulifier == 5) {
						EventType = CellUtil.cloneValue(cell)[0];
					}
				}
				System.out.println("EndTime:"+EndTime);
				System.out.println("Duration:"+Duration);
				System.out.println("Currentvalue:"+Currentvalue);
				System.out.println("Flag:"+Flag);
				System.out.println("EventType:"+EventType);
				System.out.println("DeviceId:"+DeviceId);
				System.out.println("InsertTime:"+InsertTime);
				System.out.println();
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
