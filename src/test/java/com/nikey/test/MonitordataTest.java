package com.nikey.test;

import java.io.File;
import java.io.FileWriter;
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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import com.nikey.hbase.GroupHTableMapper;
import com.nikey.hbase.HbaseTablePool;
import com.nikey.util.DateUtil;
import com.nikey.util.PropUtil;
import com.nikey.util.ScanUtil;

public class MonitordataTest {
	
	public static void main(String[] args) throws ParseException, IOException {
//		System.out.println(DateUtil.parseHHMMSSToDate("2015-07-14 19:31:46").getTime());
//		short companyId = Short.valueOf(args[0]);
//		short deviceId = Short.valueOf(args[1]);
//		new MonitordataTest().scanMonitordata(companyId, deviceId, args[2], args[3]);
//		new MonitordataTest().scanDemandAndEpi((short)2, (short)2004, "2015-03-05 00:00:00", "2015-07-15 00:00:00");
	}
	  	
	public void scanDemandAndEpi(short CompanyId, short DeviceId, String startTime, String endTime) throws ParseException, IOException {
		HTableInterface htable =  HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_monitordata_name"));
				
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
		
//		scan.setTimeRange(4, Long.MAX_VALUE);
		scan.addColumn(Bytes.toBytes("D"), new byte[]{Byte.valueOf("1")}); // 正向有功行度
		scan.addColumn(Bytes.toBytes("D"), new byte[]{Byte.valueOf("15")}); // UaShortFlicker
		int epiCount = 0;
		try {
			ResultScanner rScaner = htable.getScanner(scan);
			for (Result r : rScaner) {
				for (Cell c : r.rawCells()) {
					String family = Bytes.toString(CellUtil.cloneFamily(c));
					String qualifier = CellUtil.cloneQualifier(c)[0] + "";
					String col = family + qualifier;
					
					switch (col) {
					case "D1":
						epiCount++;
						break;
					case "D15":
						Float temp = Bytes.toFloat(CellUtil.cloneValue(c));
						System.out.println("___" + temp.toString());
						break;
					}
				}
			}
			System.out.println("epiCount : " + epiCount);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void scanMonitordata(short CompanyId, short DeviceId, String startTime, String endTime) throws ParseException, IOException {
		HTableInterface htable =  HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_monitordata_name"));
				
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
		
		int MAX = 0;
		scan.addColumn(Bytes.toBytes("D"), new byte[]{Byte.valueOf("1")}); MAX++; // 正向有功行度
		for(int i=8; i<11; i++) {
			scan.addColumn(Bytes.toBytes("D"), new byte[]{Byte.valueOf("" + i)}); MAX++; // 反向有功 正向无功 反向无功
		}
		for(int i=12; i<15; i++) {
			scan.addColumn(Bytes.toBytes("D"), new byte[]{Byte.valueOf("" + i)}); MAX++; // 峰平谷
		}
		for(int i=1; i<5; i++) {
			scan.addColumn(Bytes.toBytes("P"), new byte[]{Byte.valueOf("" + i)}); MAX++; // 有功功率
		}
		for(int i=5; i<9; i++) {
			scan.addColumn(Bytes.toBytes("P"), new byte[]{Byte.valueOf("" + i)}); MAX++; // 基波功率
		}
		for(int i=9; i<13; i++) {
			scan.addColumn(Bytes.toBytes("P"), new byte[]{Byte.valueOf("" + i)}); MAX++; // 无功功率
		}
		for(int i=13; i<17; i++) {
			scan.addColumn(Bytes.toBytes("P"), new byte[]{Byte.valueOf("" + i)}); MAX++; // 视在功率
		}
		for(int i=17; i<20; i++) {
			scan.addColumn(Bytes.toBytes("P"), new byte[]{Byte.valueOf("" + i)}); MAX++; // 单相功率因数
		}
		scan.addColumn(Bytes.toBytes("P"), new byte[]{Byte.valueOf("21")}); MAX++; // 基波功率因数
		scan.addColumn(Bytes.toBytes("P"), new byte[]{Byte.valueOf("22")}); MAX++; // 总功率因数
		for(int i=1; i<4; i++) {
			scan.addColumn(Bytes.toBytes("U"), new byte[]{Byte.valueOf("" + i)}); MAX++; // 电压
		}
		for(int i=1; i<4; i++) {
			scan.addColumn(Bytes.toBytes("I"), new byte[]{Byte.valueOf("" + i)}); MAX++; // 电流
		}
		for(int i=5; i<8; i++) {
			scan.addColumn(Bytes.toBytes("I"), new byte[]{Byte.valueOf("" + i)}); MAX++; // 基波电流
		}
		for(int i=9; i<12; i++) {
			scan.addColumn(Bytes.toBytes("I"), new byte[]{Byte.valueOf("" + i)}); MAX++; // 电流畸变
		}
		for(int i=15; i<17; i++) {
			scan.addColumn(Bytes.toBytes("I"), new byte[]{Byte.valueOf("" + i)}); MAX++; // 电流不平衡 
		}
		for(int i=2; i<26; i++) {
			scan.addColumn(Bytes.toBytes("R"), new byte[]{Byte.valueOf("" + i)}); MAX++; // A相电流谐波 
		}
		for(int i=2; i<26; i++) {
			scan.addColumn(Bytes.toBytes("S"), new byte[]{Byte.valueOf("" + i)}); MAX++; // B相电流谐波 
		}
		for(int i=2; i<26; i++) {
			scan.addColumn(Bytes.toBytes("T"), new byte[]{Byte.valueOf("" + i)}); MAX++; // C相电流谐波 
		}
		for(int i=2; i<26; i++) {
			scan.addColumn(Bytes.toBytes("X"), new byte[]{Byte.valueOf("" + i)}); MAX++; // A相电压谐波 
		}
		for(int i=2; i<26; i++) {
			scan.addColumn(Bytes.toBytes("Y"), new byte[]{Byte.valueOf("" + i)}); MAX++; // B相电压谐波 
		}
		for(int i=2; i<26; i++) {
			scan.addColumn(Bytes.toBytes("Z"), new byte[]{Byte.valueOf("" + i)}); MAX++; // C相电压谐波 
		}
			
		try {
			ResultScanner rScaner = htable.getScanner(scan);
			int count = 0;
			StringBuffer sb = new StringBuffer();
			for (Result r : rScaner) {
				for (Cell c : r.rawCells()) {
					String family = Bytes.toString(CellUtil.cloneFamily(c));
					String qualifier = CellUtil.cloneQualifier(c)[0] + "";
					String col = family + qualifier;
					count++;
					
					switch (col) {
					case "D1":
						col = "Epi";
						break;
					case "D8":
						col = "Epo";
						break;
					case "D9":
						col = "EQind";
						break;
					case "D10":
						col = "EQcap";
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
					case "P1":
						col = "Pa";
						break;
					case "P2":
						col = "Pb";
						break;
					case "P3":
						col = "Pc";
						break;
					case "P4":
						col = "P0";
						break;
					case "P5":
						col = "Pa1";
						break;
					case "P6":
						col = "Pb1";
						break;
					case "P7":
						col = "Pc1";
						break;
					case "P8":
						col = "P01";
						break;
					case "P9":
						col = "Qa";
						break;
					case "P10":
						col = "Qb";
						break;
					case "P11":
						col = "Qc";
						break;
					case "P12":
						col = "Q0";
						break;
					case "P13":
						col = "Sa";
						break;
					case "P14":
						col = "Sb";
						break;
					case "P15":
						col = "Sc";
						break;
					case "P16":
						col = "S0";
						break;
					case "P17":
						col = "Ca";
						break;
					case "P18":
						col = "Cb";
						break;
					case "P19":
						col = "Cc";
						break;
					case "P21":
						col = "PRF";
						break;
					case "P22":
						col = "PFT";
						break;
					case "U1":
						col = "Ua";
						break;
					case "U2":
						col = "Ub";
						break;
					case "U3":
						col = "Uc";
						break;
					case "I1":
						col = "Ia";
						break;
					case "I2":
						col = "Ib";
						break;
					case "I3":
						col = "Ic";
						break;
					case "I5":
						col = "Ia1";
						break;
					case "I6":
						col = "Ib1";
						break;
					case "I7":
						col = "Ic1";
						break;
					case "I9":
						col = "JBia";
						break;
					case "I10":
						col = "JBib";
						break;
					case "I11":
						col = "JBic";
						break;
					case "I15":
						col = "NBPHi";
						break;
					case "I16":
						col = "ZBPHi";
						break;
					default:
						switch (family) {
						case "R":
							family = "XBia";
							break;
						case "S":
							family = "XBib";
							break;
						case "T":
							family = "XBic";
							break;
						case "X":
							family = "XBua";
							break;
						case "Y":
							family = "XBub";
							break;
						case "Z":
							family = "XBuc";
							break;
						default:
							break;
						}
						break;
					}
					/*if("Epi".equals(col)) { // TODO update first column
						sb.append(
								ScanUtil.getDeviceIdByCell(c) +
						":" + DateUtil.formatToHHMMSS(new Date(ScanUtil.getTimeByCell(c))) +
						"	"
						+ col + ":"
						+ Bytes.toDouble(CellUtil.cloneValue(c))
								);
					} else {
						try {
							sb.append(
									"	"
									+ col + ":" 
									+ Bytes.toDouble(CellUtil.cloneValue(c))
									);
						} catch (Exception e) {
							sb.append(
									"	" 
									+ col + ":" 
									+ Bytes.toFloat(CellUtil.cloneValue(c))
									);
						}
					}
					if(count == MAX) {
						writeToFile(sb.toString());
						count = 0;
						sb = new StringBuffer();
					}*/
					try {
						System.out.println(
								"	"
								+ col + ":" 
								+ Bytes.toDouble(CellUtil.cloneValue(c))
								);
					} catch (Exception e) {
						System.out.println(
								"	" 
								+ col + ":" 
								+ Bytes.toFloat(CellUtil.cloneValue(c))
								);
					}
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void writeToFile(String msg) {
		File directory = new File("");//设定为当前文件夹
		try{
			String folder = directory.getAbsolutePath();
			String path = folder + File.separator + "data"+ ".txt";
			
			File file = new File(path);
			if(!file.getParentFile().exists()) {
			    file.getParentFile().mkdirs();
			}
			if(!file.exists()) { 
				file.createNewFile();
			}
			if(file.exists()) {
				FileWriter writer = new FileWriter(file, true);
				writer.write(msg + "\n"); // 换行
				writer.flush();
				writer.close();
				
			}
		}catch(Exception e){}
	}
}

