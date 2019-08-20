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

public class QualityTest {
	
	public static void main(String[] args) throws ParseException, IOException {
		new QualityTest().scanQualitywave(Short.valueOf(args[0]), Short.valueOf(args[1]), args[2], args[3]);
	}
	
	public void scanQualitywave(short CompanyId, short DeviceId, String startTime, String endTime) throws ParseException, IOException {
		HTableInterface htable =  HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_qualitywave_name"));
				
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
					int qua = CellUtil.cloneQualifier(c)[0];
					
					if(qua == 1) {
						System.out.println(
								(ScanUtil.getDeviceIdByCell(c)) +
								":" + ScanUtil.getTimeByCell(c) +
								":" + "PersistTime" +
								":" + Bytes.toFloat(CellUtil.cloneValue(c)));
					} else if(qua == 2) {
						System.out.println(
								(ScanUtil.getDeviceIdByCell(c)) +
								":" + ScanUtil.getTimeByCell(c) +
								":" + "HappenTime" +
								":" + Bytes.toLong(CellUtil.cloneValue(c)));
					} else if(qua == 3) {
						System.out.println(
								(ScanUtil.getDeviceIdByCell(c)) +
								":" + ScanUtil.getTimeByCell(c) +
								":" + "Eua" +
								":" + Bytes.toFloat(CellUtil.cloneValue(c)));
					} else if(qua == 4) {
						System.out.println(
								(ScanUtil.getDeviceIdByCell(c)) +
								":" + ScanUtil.getTimeByCell(c) +
								":" + "Eub" +
								":" + Bytes.toFloat(CellUtil.cloneValue(c)));
					} else if(qua == 5) {
						System.out.println(
								(ScanUtil.getDeviceIdByCell(c)) +
								":" + ScanUtil.getTimeByCell(c) +
								":" + "Euc" +
								":" + Bytes.toFloat(CellUtil.cloneValue(c)));
					} else if(qua == 6) {
						System.out.println(
								(ScanUtil.getDeviceIdByCell(c)) +
								":" + ScanUtil.getTimeByCell(c) +
								":" + "CauseId" +
								":" + CellUtil.cloneValue(c)[0]);
					} else if(qua == 7) {
						byte[] data = CellUtil.cloneValue(c);
						byte[] temp = new byte[] {
								data[data.length - 4],
								data[data.length - 3],
								data[data.length - 2],
								data[data.length - 1]
						};
						System.out.println(
								(ScanUtil.getDeviceIdByCell(c)) +
								":" + ScanUtil.getTimeByCell(c) +
								":" + "Ua" +
								":" + Bytes.toFloat(temp));
					} else if(qua == 8) {
						byte[] data = CellUtil.cloneValue(c);
						byte[] temp = new byte[] {
								data[data.length - 4],
								data[data.length - 3],
								data[data.length - 2],
								data[data.length - 1]
						};
						System.out.println(
								(ScanUtil.getDeviceIdByCell(c)) +
								":" + ScanUtil.getTimeByCell(c) +
								":" + "Ub" +
								":" + Bytes.toFloat(temp));
					} else if(qua == 9) {
						byte[] data = CellUtil.cloneValue(c);
						byte[] temp = new byte[] {
								data[data.length - 4],
								data[data.length - 3],
								data[data.length - 2],
								data[data.length - 1]
						};
						System.out.println(
								(ScanUtil.getDeviceIdByCell(c)) +
								":" + ScanUtil.getTimeByCell(c) +
								":" + "Uc" +
								":" + Bytes.toFloat(temp));
					} else if(qua == 10) {
						byte[] data = CellUtil.cloneValue(c);
						byte[] temp = new byte[] {
								data[data.length - 4],
								data[data.length - 3],
								data[data.length - 2],
								data[data.length - 1]
						};
						System.out.println(
								(ScanUtil.getDeviceIdByCell(c)) +
								":" + ScanUtil.getTimeByCell(c) +
								":" + "Ia" +
								":" + Bytes.toFloat(temp));
					} else if(qua == 11) {
						byte[] data = CellUtil.cloneValue(c);
						byte[] temp = new byte[] {
								data[data.length - 4],
								data[data.length - 3],
								data[data.length - 2],
								data[data.length - 1]
						};
						System.out.println(
								(ScanUtil.getDeviceIdByCell(c)) +
								":" + ScanUtil.getTimeByCell(c) +
								":" + "Ib" +
								":" + Bytes.toFloat(temp));
					} else if(qua == 12) {
						byte[] data = CellUtil.cloneValue(c);
						byte[] temp = new byte[] {
								data[data.length - 4],
								data[data.length - 3],
								data[data.length - 2],
								data[data.length - 1]
						};
						System.out.println(
								(ScanUtil.getDeviceIdByCell(c)) +
								":" + ScanUtil.getTimeByCell(c) +
								":" + "Ic" +
								":" + Bytes.toFloat(temp));
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
