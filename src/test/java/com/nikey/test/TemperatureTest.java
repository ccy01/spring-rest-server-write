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
import com.nikey.util.PropUtil;
import com.nikey.util.ScanUtil;

public class TemperatureTest {
	
	public static void main(String[] args) throws ParseException, IOException {
		new TemperatureTest().scanTemperature((short)3, (short)3003, "2015-01-17 11:00:00", "2015-01-19 15:05:00");
	}
	
	public void scanTemperature(short CompanyId, short DeviceId, String startTime, String endTime) throws ParseException, IOException {
		HTableInterface htable =  HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_temperature_name"));
				
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
		
		scan.setCaching(100);
		scan.setBatch(2);
		
		try {
			ResultScanner rScaner = htable.getScanner(scan);
			for (Result r : rScaner) {
				for (Cell c : r.rawCells()) {

					try {
						System.out.println(
								(ScanUtil.getDeviceIdByCell(c)) +
								":" + ScanUtil.getTimeByCell(c) +
								":" + CellUtil.cloneQualifier(c)[0] +
								":" + Bytes.toLong(CellUtil.cloneValue(c)));
					} catch (Exception e) {
						try {
							System.out.println(
									(ScanUtil.getDeviceIdByCell(c)) +
									":" + ScanUtil.getTimeByCell(c) +
									":" + CellUtil.cloneQualifier(c)[0] +
									":" + Bytes.toFloat(CellUtil.cloneValue(c)));
						} catch (Exception e2) {
							System.out.println(
									(ScanUtil.getDeviceIdByCell(c)) +
									":" + ScanUtil.getTimeByCell(c) +
									":" + CellUtil.cloneQualifier(c)[0] +
									":" + CellUtil.cloneValue(c)[0]);
						}
					}
				
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
