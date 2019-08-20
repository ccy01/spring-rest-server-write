package com.nikey.test;

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
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.nikey.hbase.CommerrHTableMapper;
import com.nikey.hbase.HbaseTablePool;
import com.nikey.util.DateUtil;
import com.nikey.util.PropUtil;
import com.nikey.util.ScanUtil;

public class ScanDegree {
	
	public static void main(String[] args) {
		ScanDegree scanDegree = new ScanDegree();
		/*short CompanyId = 4;
		short DeviceId = 4004;
		String startTime = "2016-01-02 00:00:00";
		String endTime = "2016-01-03 00:00:00";*/
		
		/*short CompanyId = 1;
		short DeviceId = 1002;
		String startTime = "2015-02-22 23:55:00";
		String endTime = "2015-02-23 00:05:00";*/
		
		short DeviceId = 1001, cases = 1;
		String startTime = "2014-12-01 01:00:00";
		String endTime = "2015-01-31 01:01:00";
		
		try {
//			scanDegree.scanDegree(CompanyId, DeviceId, startTime, endTime);
//			scanDegree.scanMonitordata((short) (DeviceId/1000), DeviceId, startTime, endTime);
//			scanDegree.scanTimeInterval((short) (DeviceId/1000), DeviceId, startTime, endTime);
//			scanDegree.scanTimeIntervalOfTemperature((short) (DeviceId/1000), DeviceId, startTime, endTime, (int) cases);
			scanDegree.scanDegreevalue((short) (DeviceId/1000), DeviceId, startTime, endTime);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void scanDegreevalue(short CompanyId, short DeviceId, String startTime, String endTime) throws ParseException, IOException {
		HTableInterface htable =  HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_degreevalue_name"));
				
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		Scan scan = null;
		try {
			byte[] startKey = Bytes.add(Bytes.toBytes(CompanyId),
					Bytes.toBytes(DeviceId),
					Bytes.toBytes(format.parse(startTime).getTime())
				);
			byte[] endKey = Bytes.add(Bytes.toBytes(CompanyId),
					Bytes.toBytes(DeviceId),
					Bytes.toBytes(format.parse(endTime).getTime())
				);
			scan = new Scan(startKey, endKey);
		} catch (Exception e) {
			byte[] startKey = Bytes.add(Bytes.toBytes(CompanyId),
					Bytes.toBytes(DeviceId),
					Bytes.toBytes(Long.valueOf(startTime))
				);
			byte[] endKey = Bytes.add(Bytes.toBytes(CompanyId),
					Bytes.toBytes(DeviceId),
					Bytes.toBytes(Long.valueOf(endTime))
				);
			scan = new Scan(startKey, endKey);
		}
		
//		scan.addFamily(Bytes.toBytes("A"));
		
//		scan.addColumn(Bytes.toBytes("A"), new byte[]{Byte.valueOf("1")});
//		scan.addColumn(Bytes.toBytes("A"), new byte[]{Byte.valueOf("2")});
//		scan.addColumn(Bytes.toBytes("A"), new byte[]{Byte.valueOf("3")});
		scan.addColumn(Bytes.toBytes("A"), new byte[]{Byte.valueOf("4")});
//		scan.addColumn(Bytes.toBytes("A"), new byte[]{Byte.valueOf("5")});
//		scan.addColumn(Bytes.toBytes("A"), new byte[]{Byte.valueOf("6")});
//		scan.addColumn(Bytes.toBytes("A"), new byte[]{Byte.valueOf("7")});
		
		scan.setCaching(100);
		scan.setBatch(2);
		
		try {
			ResultScanner rScaner = htable.getScanner(scan);
			for (Result r : rScaner) {
				for (Cell cell : r.rawCells()) {
					String col = Bytes.toString(CellUtil.cloneFamily(cell)) + CellUtil.cloneQualifier(cell)[0];
					byte timeType = ScanUtil.getDegreeType(cell);
					if(timeType != 1) {
						continue;
					}
					try {
						System.out.println(
								(ScanUtil.getDeviceIdByCell(cell)) +
								":" + DateUtil.formatToHHMMSS(new Date(ScanUtil.getTimeByCell(cell))) +
								":" + col +
								":" + Bytes.toDouble(CellUtil.cloneValue(cell)) +
								":" + timeType);
					} catch (Exception e) {
						System.out.println(
								(ScanUtil.getDeviceIdByCell(cell)) +
								":" + DateUtil.formatToHHMMSS(new Date(ScanUtil.getTimeByCell(cell))) +
								":" + col +
								":" + Bytes.toFloat(CellUtil.cloneValue(cell)) +
								":" + timeType);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void scanDegree(short CompanyId, short DeviceId, String startTime, String endTime) throws ParseException, IOException {
		HTableInterface htable =  HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_degreevalue_name"));
				
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		Scan scan = null;
		try {
			byte[] startKey = Bytes.add(Bytes.toBytes(CompanyId),
					Bytes.toBytes(DeviceId),
					Bytes.toBytes(format.parse(startTime).getTime())
				);
			byte[] endKey = Bytes.add(Bytes.toBytes(CompanyId),
					Bytes.toBytes(DeviceId),
					Bytes.toBytes(format.parse(endTime).getTime())
				);
			scan = new Scan(startKey, endKey);
		} catch (Exception e) {
			byte[] startKey = Bytes.add(Bytes.toBytes(CompanyId),
					Bytes.toBytes(DeviceId),
					Bytes.toBytes(Long.valueOf(startTime))
				);
			byte[] endKey = Bytes.add(Bytes.toBytes(CompanyId),
					Bytes.toBytes(DeviceId),
					Bytes.toBytes(Long.valueOf(endTime))
				);
			scan = new Scan(startKey, endKey);
		}
		
//		scan.addColumn(Bytes.toBytes("A"), new byte[]{Byte.valueOf("1")});
//		scan.addColumn(Bytes.toBytes("A"), new byte[]{Byte.valueOf("2")});
//		scan.addColumn(Bytes.toBytes("A"), new byte[]{Byte.valueOf("3")});
		scan.addColumn(Bytes.toBytes("A"), new byte[]{Byte.valueOf("4")});
//		scan.addColumn(Bytes.toBytes("A"), new byte[]{Byte.valueOf("5")});
//		scan.addColumn(Bytes.toBytes("A"), new byte[]{Byte.valueOf("6")});
//		scan.addColumn(Bytes.toBytes("A"), new byte[]{Byte.valueOf("7")});
		
		scan.setCaching(100);
		scan.setBatch(2);
		
		try {
			ResultScanner rScaner = htable.getScanner(scan);
			int count = 0;
			for (Result r : rScaner) {
				for (Cell c : r.rawCells()) {
					String col = Bytes.toString(CellUtil.cloneFamily(c)) + CellUtil.cloneQualifier(c)[0];
					count++;
					byte timeType = ScanUtil.getDegreeType(c);
					if(timeType == 0) 
						continue;
					System.out.println(
							(ScanUtil.getDeviceIdByCell(c)) +
							":" + DateUtil.formatToHHMMSS(new Date(ScanUtil.getTimeByCell(c))) +
							":" + col +
							":" + Bytes.toDouble(CellUtil.cloneValue(c)) +
							":" + timeType/* +
							":" + count*/);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void scanMonitordata(short CompanyId, short DeviceId, String startTime, String endTime) throws ParseException, IOException {
		HTableInterface htable =  HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_monitordata_name"));
				
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		Scan scan = null;
		try {
			byte[] startKey = Bytes.add(Bytes.toBytes(CompanyId),
					Bytes.toBytes(DeviceId),
					Bytes.toBytes(format.parse(startTime).getTime())
				);
			byte[] endKey = Bytes.add(Bytes.toBytes(CompanyId),
					Bytes.toBytes(DeviceId),
					Bytes.toBytes(format.parse(endTime).getTime())
				);
			scan = new Scan(startKey, endKey);
		} catch (Exception e) {
			byte[] startKey = Bytes.add(Bytes.toBytes(CompanyId),
					Bytes.toBytes(DeviceId),
					Bytes.toBytes(Long.valueOf(startTime))
				);
			byte[] endKey = Bytes.add(Bytes.toBytes(CompanyId),
					Bytes.toBytes(DeviceId),
					Bytes.toBytes(Long.valueOf(endTime))
				);
			scan = new Scan(startKey, endKey);
		}
		
		scan.addColumn(Bytes.toBytes("D"), new byte[]{Byte.valueOf("1")});
//		scan.addColumn(Bytes.toBytes("D"), new byte[]{Byte.valueOf("12")});
//		scan.addColumn(Bytes.toBytes("D"), new byte[]{Byte.valueOf("13")});
//		scan.addColumn(Bytes.toBytes("D"), new byte[]{Byte.valueOf("14")});
		
		scan.setCaching(100);
		scan.setBatch(2);
		
		try {
			ResultScanner rScaner = htable.getScanner(scan);
			Double cache = 0.0;
			for (Result r : rScaner) {
				for (Cell c : r.rawCells()) {
					String col = Bytes.toString(CellUtil.cloneFamily(c)) + CellUtil.cloneQualifier(c)[0];
					switch (col) {
					case "D1":
						col = "Epi";
						break;
					case "D12":
						col = "PeakEpi";
						break;
					case "D13":
						col = "FlatEpi";
						break;
					case "D14":
						col = "ValleyEpi";
						break;
					}
					Double value = Bytes.toDouble(CellUtil.cloneValue(c));
					if(CellUtil.cloneQualifier(c)[0] == 1) {
						if(cache > value) {
							System.out.println("----");
							fileRecord(
								DeviceId,
								"----"
							);
						}
						cache = value;
					}
					System.out.println(
							(ScanUtil.getDeviceIdByCell(c)) +
							":" + DateUtil.formatToHHMMSS(new Date(ScanUtil.getTimeByCell(c))) +
							":[" + col +
							":" + value +
							"]");
					fileRecord(
							DeviceId,
							(ScanUtil.getDeviceIdByCell(c)) +
							":" + DateUtil.formatToHHMMSS(new Date(ScanUtil.getTimeByCell(c))) +
							":[" + col +
							":" + value +
							"]");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void scanTimeInterval(short CompanyId, short DeviceId, String startTime, String endTime) throws ParseException, IOException {
		HTableInterface htable =  HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_monitordata_name"));
				
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		Scan scan = null;
		try {
			byte[] startKey = Bytes.add(Bytes.toBytes(CompanyId),
					Bytes.toBytes(DeviceId),
					Bytes.toBytes(format.parse(startTime).getTime())
				);
			byte[] endKey = Bytes.add(Bytes.toBytes(CompanyId),
					Bytes.toBytes(DeviceId),
					Bytes.toBytes(format.parse(endTime).getTime())
				);
			scan = new Scan(startKey, endKey);
		} catch (Exception e) {
			byte[] startKey = Bytes.add(Bytes.toBytes(CompanyId),
					Bytes.toBytes(DeviceId),
					Bytes.toBytes(Long.valueOf(startTime))
				);
			byte[] endKey = Bytes.add(Bytes.toBytes(CompanyId),
					Bytes.toBytes(DeviceId),
					Bytes.toBytes(Long.valueOf(endTime))
				);
			scan = new Scan(startKey, endKey);
		}
		
		scan.addColumn(Bytes.toBytes("D"), new byte[]{Byte.valueOf("1")});
		
		scan.setCaching(100);
		scan.setBatch(2);
		
		try {
			ResultScanner rScaner = htable.getScanner(scan);
			CommerrHTableMapper mapper = new CommerrHTableMapper();
			List<Put> puts = new ArrayList<Put>();
			long cache = 0l;
			for (Result r : rScaner) {
				for (Cell c : r.rawCells()) {
					if(CellUtil.cloneQualifier(c)[0] == 1) {
						long value = ScanUtil.getTimeByCell(c);
						if(cache != 0) {
							long sub = value - cache;
							if(sub >= 300000) {
								System.out.println(
										(ScanUtil.getDeviceIdByCell(c)) +
										":" + DateUtil.formatToHHMMSS(new Date(ScanUtil.getTimeByCell(c))) +
										":[" + (sub) +
										"]");
								fileRecord(
										DeviceId,
										(ScanUtil.getDeviceIdByCell(c)) +
										":[" + (sub) +
										"]");
								Put put = mapper.getPutForMonitordataFix(CompanyId, DeviceId, value - 1000);
								puts.add(put);
							}
						}
						cache = value;
					}
				}
			}
			System.out.println(puts.size());
			htable.put(puts);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void scanTimeIntervalOfTemperature(short CompanyId, short DeviceId, String startTime, String endTime, Integer cases) throws ParseException, IOException {
		HTableInterface htable =  HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_temperature_name"));
				
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		Scan scan = null;
		try {
			byte[] startKey = Bytes.add(Bytes.toBytes(CompanyId),
					Bytes.toBytes(DeviceId),
					Bytes.toBytes(format.parse(startTime).getTime())
				);
			byte[] endKey = Bytes.add(Bytes.toBytes(CompanyId),
					Bytes.toBytes(DeviceId),
					Bytes.toBytes(format.parse(endTime).getTime())
				);
			scan = new Scan(startKey, endKey);
		} catch (Exception e) {
			byte[] startKey = Bytes.add(Bytes.toBytes(CompanyId),
					Bytes.toBytes(DeviceId),
					Bytes.toBytes(Long.valueOf(startTime))
				);
			byte[] endKey = Bytes.add(Bytes.toBytes(CompanyId),
					Bytes.toBytes(DeviceId),
					Bytes.toBytes(Long.valueOf(endTime))
				);
			scan = new Scan(startKey, endKey);
		}
		
		scan.addColumn(Bytes.toBytes("A"), new byte[]{Byte.valueOf(cases.toString())});
		
		scan.setCaching(100);
		scan.setBatch(2);
		
		try {
			ResultScanner rScaner = htable.getScanner(scan);
			CommerrHTableMapper mapper = new CommerrHTableMapper();
			List<Put> puts = new ArrayList<Put>();
			long cache = 0l;
			for (Result r : rScaner) {
				for (Cell c : r.rawCells()) {
					if(CellUtil.cloneQualifier(c)[0] == cases) {
						long value = ScanUtil.getTimeByCell(c);
						if(cache != 0) {
							long sub = value - cache;
							if(sub >= 300000) {
								System.out.println(
										(ScanUtil.getDeviceIdByCell(c)) +
										":" + DateUtil.formatToHHMMSS(new Date(ScanUtil.getTimeByCell(c))) +
										":[" + (sub) +
										"]");
								fileRecord(
										DeviceId,
										(ScanUtil.getDeviceIdByCell(c)) +
										":[" + (sub) +
										"]");
								Put put = mapper.getPutForTemperatureFix(CompanyId, DeviceId, value - 1000, cases);
								puts.add(put);
							}
						}
						cache = value;
					}
				}
			}
			System.out.println(puts.size());
			htable.put(puts);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void fileRecord(short filename, String msg) {
		File directory = new File("");//设定为当前文件夹
		try{
			String folder = directory.getAbsolutePath();
			String path = folder + File.separator + filename + ".txt";
			
			File file = new File(path);
			if(!file.getParentFile().exists()) {
			    file.getParentFile().mkdirs();
			}
			if(!file.exists()) { 
				file.createNewFile();
			}
			if(file.exists()) {
				FileWriter writer = new FileWriter(file, true);
				writer.write(msg + "\n");
				writer.flush();
				writer.close();
			}
		}catch(Exception e){}
	}

}
