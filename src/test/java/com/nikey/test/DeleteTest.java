package com.nikey.test;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.nikey.hbase.HbaseTablePool;
import com.nikey.util.DateUtil;
import com.nikey.util.ScanUtil;

public class DeleteTest {
	
	public static void main(String[] args) throws NumberFormatException, IOException {
		DeleteTest test = new DeleteTest();
		short StartDeviceID = 4001, EndDeviceID = 4004;
		String StartTime = "2015-10-01 00:00:00", EndTime = "2016-10-01 00:00:00";
		for(short DeviceID = StartDeviceID; DeviceID <= EndDeviceID; DeviceID++) {
//			test.del((short) (DeviceID / 1000), DeviceID, StartTime, EndTime, "monitordata", "D", "1");
//			test.del((short) (DeviceID / 1000), DeviceID, StartTime, EndTime, "alarmdata", "A", "1");
//			test.del((short) (DeviceID / 1000), DeviceID, StartTime, EndTime, "alarmwave", "A", "1");
			test.del((short) (DeviceID / 1000), DeviceID, StartTime, EndTime, "qualitywave", "A", "1");
//			test.del((short) (DeviceID / 1000), DeviceID, StartTime, EndTime, "degreevalue", "A", "1");
//			test.del((short) (DeviceID / 1000), DeviceID, StartTime, EndTime, "demandvalue", "A", "1");
		}
	}
	
	public void del(short CompanyId, short DeviceId, String StartTime, String EndTime, String table, String family, String qualifier) throws IOException {
		HTableInterface htable =  HbaseTablePool.instance().getHtable(table);
				
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		Scan scan = null;
		try {
			byte[] startKey = Bytes.add(Bytes.toBytes((short)CompanyId), Bytes
					.toBytes((short)DeviceId), Bytes.toBytes(format.parse(StartTime).getTime()));
			byte[] endKey = Bytes.add(Bytes.toBytes((short)CompanyId), Bytes
					.toBytes((short)DeviceId), Bytes.toBytes(format.parse(EndTime).getTime()));
			 scan = new Scan(startKey, endKey);
		} catch (Exception e) {
			byte[] startKey = Bytes.add(Bytes.toBytes((short)CompanyId), Bytes
					.toBytes((short)DeviceId), Bytes.toBytes(Long.valueOf(StartTime)));
			byte[] endKey = Bytes.add(Bytes.toBytes((short)CompanyId), Bytes
					.toBytes((short)DeviceId), Bytes.toBytes(Long.valueOf(EndTime)));
			scan = new Scan(startKey, endKey);
		}
		
		scan.addColumn(Bytes.toBytes(family), new byte[]{Byte.valueOf(qualifier)}); // 正向有功行度
		try {
			ResultScanner rScaner = htable.getScanner(scan);
			for (Result r : rScaner) {
				for (Cell c : r.rawCells()) {
					System.out.println(DeviceId + ", " + DateUtil.formatToHHMMSS(ScanUtil.getTimeByCell(c)));
					// TODO 组装成Delete链表删除
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
