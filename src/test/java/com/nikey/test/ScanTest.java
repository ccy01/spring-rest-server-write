package com.nikey.test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.nikey.hbase.HbaseTablePool;
import com.nikey.util.DateUtil;
import com.nikey.util.JsonUtil;
import com.nikey.util.PropUtil;
import com.nikey.util.ScanUtil;

public class ScanTest {
	
	public static void main(String[] args) throws ParseException, IOException {
//		String str = "286.16467,261.70938,229.09734,180.10365,120.942635,69.41323,29.417496,-13.415397,-65.62978,-120.05445,-170.13274,-220.45792,-253.50735,-277.21075,-301.47787,-323.35547,-333.77335,-332.879,-323.40463,-304.5825,-286.20105,-262.01266,-227.86072,-178.66905,-120.291,-69.42713,-28.239164,14.605979,65.83098,119.286674,170.23238,221.85022,254.63245,277.55286,301.29483,323.41003,333.49258,332.6395,322.59744,304.23874,285.75638,261.2374,228.43799,179.20914,119.49117,67.73486,27.945274,-14.29932,-66.185936,-119.990685,-171.1454,-221.46477,-254.22691,-277.52924,-301.77148,-323.85483,-334.03143,-332.93573,-323.18982,-304.56165,-285.86652,-261.52374,-227.33705,-177.46127,-118.449486,-69.020226,-28.38615,15.176989,67.10096,121.48037,172.0322,222.74265,255.2792,277.62793,301.48187,323.82318,334.25262,332.98438,322.58447,303.71814,285.3266,260.50793,227.34991,177.77652,118.67796,67.24569,27.278011,-15.062057,-66.77962,-120.67854,-171.0773,-221.99295,-255.1806,-278.24185,-302.46082,-324.10492,-333.3318,-332.39304,-322.6875,-304.09064,-285.66037,-261.3599,-226.74884,-176.74179,-118.3508,-68.07306,-27.385906,15.833568,67.201035,120.884865,172.03645,223.37204,255.82762,278.1343,301.94403,323.9548,333.85864,333.0577,322.52542,304.20688,285.36258,260.0595,226.96169,176.64024,117.71967,67.30267,27.419178,-15.695528,-68.1118,-122.3285,-172.76566,-222.5584,-255.30708,-278.26096,-302.64343,-324.41592,-334.00583,-332.6284,-322.46356,-303.89343,-285.14355,-260.37296,-225.80515,-175.87828,-117.2947,-67.60612,-26.818283,16.579191,68.24967,121.79033,172.51599,223.55351,255.85777,278.42496,302.43216,324.13724,333.74878,332.42596,321.6222,303.19464,284.8112,259.9365,226.32474,176.43436,117.167694,66.61908,27.035463,-15.896377,-67.97246,-122.00473,-172.90118,-223.39801,-255.40161,-278.297,-302.69025,-324.3518,-333.81818,-332.82706,-321.8702,-304.06613,-284.86948,-260.34412,-225.85188,-175.82964,-117.496506,-67.50083,-26.773718,16.747723,69.29161,121.64899,173.51805,223.67366,255.38745,278.17096,302.3974,324.3551,333.9993,332.64166,321.3872,303.3662,284.33472,259.22928,225.98567,176.0582,116.75713,66.12539,26.232506,-16.463305,-68.58473,-122.398,-172.58844,-223.27509,-255.57248,-278.71207,-302.97006,-324.4188,-333.70743,-332.519,-321.89462,-303.40668,-284.44287,-260.03015,-225.56877,-175.37938,-116.71717,-66.88404,-25.976318,17.432535,69.59723,121.904305,174.04788,224.39713,255.84517,278.54538,302.80032,324.35693,333.98602,332.71567,321.30807,302.9956,284.2547,259.05606,225.74373,175.25706,115.59956,65.80561,26.397388,-17.114496,-69.63476,-123.895935,-173.75644,-223.5887,-255.65944,-278.97705,-303.29617,-324.75046,-333.96942,-332.603,-321.46558,-303.0698,-284.21533,-259.3231,-224.95615,-174.59918,-115.96947,-66.33892,-24.874607,18.65526,70.172325,122.39739,174.50961,224.96259,256.50793,278.83362,303.353,324.70148,333.6779,332.36942,321.1637,302.73386,283.88037,258.42493,225.114,174.56451,114.51723,64.655685,25.494093,-17.775246,-70.32243,-124.28104,-174.61942,-224.69232,-256.46744,-279.3797,-303.85513,-324.88538,-333.85727,-332.58694,-321.58737,-303.02887,-284.15063,-258.963,-224.34344,-173.06302,-114.61269,-65.92871,-24.630497,19.139572,71.14286,123.47921,175.71358,225.6626,256.4829,279.1816,303.4522,324.95148,334.06766,332.40695,320.40842,302.23523,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,";
//		String [] arr = str.split(",");
//		System.out.println(arr[319]);
//		System.out.println(arr[320]);
//		System.out.println(arr[321]);
//		new ScanTest().scanQualitywave((short)1, (short)1003, "2014-11-11 03:44:22", "2014-11-11 03:44:23");
//		new ScanTest().scanDemand((short)1, (short)1005, "2014-10-21 12:51:06", "2014-10-30 12:51:06");
//		new ScanTest().scanGroup((short)1, (short)1004, "1413484502000", "1413484502001");
		new ScanTest().scanAlarmwave((short)9, (short)9007, "2002-01-14 00:09:09", "2002-02-14 00:09:09");
//		new ScanTest().scanMonitordata((short)1, (short)1001, "2015-05-11 00:00:00", "2015-05-12 00:00:00");		
//		new ScanTest().scanDegree((short)1, (short)1005, "2014-10-22 12:00:00", "2014-10-22 14:00:00");
//		new ScanTest().scanAlarmdata((short)1, (short)1001, "1413710100000", "1413710100001");
//		new ScanTest().addQulitywave();
//		System.out.println(new Date(1413508897964l)); // h1:1413508897964,	
		/*ScanTest scan = new ScanTest();
		for(short i=1001; i<=1024; i++) {
			Set<String> set = new ScanTest().scanMonitordata((short) 1, (short) i, "2014-10-21 00:00:00", "2014-10-29 00:00:00");
			if(set != null) {
				scan.delete(set);
			}			
		}*/
	}
	
	public void delete(Set<String> set) throws NumberFormatException, IOException {
		for(String val : set) {
			delMonitordata((short) 1, Short.valueOf(val.split(":")[0]), val.split(":")[1]);
		}
		System.out.println(set.size() + " done ...");
	}
	
	public void delMonitordata(short CompanyId, short DeviceId, String InsertTime) throws IOException {
		HTableInterface htable =  HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_monitordata_name"));
		
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		Delete delete = null;
		try {
			byte[] startKey = Bytes.add(Bytes.toBytes((short)CompanyId), Bytes
					.toBytes((short)DeviceId), Bytes.toBytes(format.parse(InsertTime).getTime()));
			delete = new Delete(startKey);
		} catch (Exception e) {
			byte[] startKey = Bytes.add(Bytes.toBytes((short)CompanyId), Bytes
					.toBytes((short)DeviceId), Bytes.toBytes(Long.valueOf(InsertTime)));
			delete = new Delete(startKey);
		}
		
		htable.delete(delete);
	}
	
	/**
	 * @date 28 Sep, 2014
	 * @throws ParseException
	 *	test method
	 * @throws IOException 
	 */
	public Set<String> scanMonitordata(short CompanyId, short DeviceId, String startTime, String endTime) throws ParseException, IOException {
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
		
		scan.addColumn(Bytes.toBytes("I"), new byte[]{Byte.valueOf("9")});
		scan.addColumn(Bytes.toBytes("I"), new byte[]{Byte.valueOf("10")});
		scan.addColumn(Bytes.toBytes("I"), new byte[]{Byte.valueOf("11")});
		
//		scan.addColumn(Bytes.toBytes("U"), new byte[]{Byte.valueOf("1")});
		
		/*scan.addColumn(Bytes.toBytes("U"), new byte[]{Byte.valueOf("1")});
		scan.addColumn(Bytes.toBytes("U"), new byte[]{Byte.valueOf("2")});
		scan.addColumn(Bytes.toBytes("U"), new byte[]{Byte.valueOf("3")});*/
		
		/*scan.addColumn(Bytes.toBytes("P"), new byte[]{Byte.valueOf("1")});
		scan.addColumn(Bytes.toBytes("P"), new byte[]{Byte.valueOf("2")});
		scan.addColumn(Bytes.toBytes("P"), new byte[]{Byte.valueOf("3")});
		scan.addColumn(Bytes.toBytes("P"), new byte[]{Byte.valueOf("3")});
		scan.addColumn(Bytes.toBytes("P"), new byte[]{Byte.valueOf("5")});
		scan.addColumn(Bytes.toBytes("P"), new byte[]{Byte.valueOf("6")});
		scan.addColumn(Bytes.toBytes("P"), new byte[]{Byte.valueOf("7")});
		scan.addColumn(Bytes.toBytes("P"), new byte[]{Byte.valueOf("8")});
		scan.addColumn(Bytes.toBytes("P"), new byte[]{Byte.valueOf("9")});
		scan.addColumn(Bytes.toBytes("P"), new byte[]{Byte.valueOf("10")});
		scan.addColumn(Bytes.toBytes("P"), new byte[]{Byte.valueOf("11")});
		scan.addColumn(Bytes.toBytes("P"), new byte[]{Byte.valueOf("12")});
		scan.addColumn(Bytes.toBytes("P"), new byte[]{Byte.valueOf("13")});
		scan.addColumn(Bytes.toBytes("P"), new byte[]{Byte.valueOf("14")});
		scan.addColumn(Bytes.toBytes("P"), new byte[]{Byte.valueOf("15")});
		scan.addColumn(Bytes.toBytes("P"), new byte[]{Byte.valueOf("16")});
		scan.addColumn(Bytes.toBytes("P"), new byte[]{Byte.valueOf("17")});
		scan.addColumn(Bytes.toBytes("P"), new byte[]{Byte.valueOf("18")});
		scan.addColumn(Bytes.toBytes("P"), new byte[]{Byte.valueOf("19")});
		scan.addColumn(Bytes.toBytes("P"), new byte[]{Byte.valueOf("22")});
		scan.addColumn(Bytes.toBytes("U"), new byte[]{Byte.valueOf("1")});
		scan.addColumn(Bytes.toBytes("U"), new byte[]{Byte.valueOf("2")});
		scan.addColumn(Bytes.toBytes("U"), new byte[]{Byte.valueOf("3")});
		scan.addColumn(Bytes.toBytes("I"), new byte[]{Byte.valueOf("1")});
		scan.addColumn(Bytes.toBytes("I"), new byte[]{Byte.valueOf("2")});
		scan.addColumn(Bytes.toBytes("I"), new byte[]{Byte.valueOf("3")});
		scan.addColumn(Bytes.toBytes("I"), new byte[]{Byte.valueOf("5")});
		scan.addColumn(Bytes.toBytes("I"), new byte[]{Byte.valueOf("6")});
		scan.addColumn(Bytes.toBytes("I"), new byte[]{Byte.valueOf("7")});
		scan.addColumn(Bytes.toBytes("I"), new byte[]{Byte.valueOf("9")});
		scan.addColumn(Bytes.toBytes("I"), new byte[]{Byte.valueOf("10")});
		scan.addColumn(Bytes.toBytes("I"), new byte[]{Byte.valueOf("11")});
		scan.addColumn(Bytes.toBytes("I"), new byte[]{Byte.valueOf("15")});
		scan.addColumn(Bytes.toBytes("I"), new byte[]{Byte.valueOf("16")});
		scan.addColumn(Bytes.toBytes("D"), new byte[]{Byte.valueOf("1")});
		scan.addColumn(Bytes.toBytes("D"), new byte[]{Byte.valueOf("12")});
		scan.addColumn(Bytes.toBytes("D"), new byte[]{Byte.valueOf("13")});
		scan.addColumn(Bytes.toBytes("D"), new byte[]{Byte.valueOf("14")});*/
		
		/*scan.addColumn(Bytes.toBytes("R"), new byte[]{Byte.valueOf("2")});
		scan.addColumn(Bytes.toBytes("R"), new byte[]{Byte.valueOf("3")});
		scan.addColumn(Bytes.toBytes("R"), new byte[]{Byte.valueOf("3")});
		scan.addColumn(Bytes.toBytes("R"), new byte[]{Byte.valueOf("5")});
		scan.addColumn(Bytes.toBytes("R"), new byte[]{Byte.valueOf("6")});
		scan.addColumn(Bytes.toBytes("R"), new byte[]{Byte.valueOf("7")});
		scan.addColumn(Bytes.toBytes("R"), new byte[]{Byte.valueOf("8")});
		scan.addColumn(Bytes.toBytes("R"), new byte[]{Byte.valueOf("9")});
		scan.addColumn(Bytes.toBytes("R"), new byte[]{Byte.valueOf("10")});
		scan.addColumn(Bytes.toBytes("R"), new byte[]{Byte.valueOf("11")});
		scan.addColumn(Bytes.toBytes("R"), new byte[]{Byte.valueOf("12")});
		scan.addColumn(Bytes.toBytes("R"), new byte[]{Byte.valueOf("13")});
		scan.addColumn(Bytes.toBytes("R"), new byte[]{Byte.valueOf("14")});
		scan.addColumn(Bytes.toBytes("R"), new byte[]{Byte.valueOf("15")});*/		
		
		try {
			ResultScanner rScaner = htable.getScanner(scan);
			int count = 0;
			Map<String, String> errorRecord = new HashMap<>();
//			int MAX = 34;
//			StringBuffer sb = new StringBuffer();
			for (Result r : rScaner) {
				for (Cell c : r.rawCells()) {
					String col = Bytes.toString(CellUtil.cloneFamily(c)) + CellUtil.cloneQualifier(c)[0];
					count++;
					/*
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
						col = "NBPHu";
						break;
					default:
						break;
					}
					if("Ia".equals(col)) {
						sb.append(
								ScanUtil.getDeviceIdByCell(c) +
						":" + DateUtil.formatToHHMMSS(new Date(ScanUtil.getTimeByCell(c))) +
						"	" + col +
						":" + Bytes.toFloat(CellUtil.cloneValue(c))
								);
					} else {
						try {
							sb.append(
									"	" + col +
									":" + Bytes.toDouble(CellUtil.cloneValue(c))
									);
						} catch (Exception e) {
							sb.append(
									"	" + col +
									":" + Bytes.toFloat(CellUtil.cloneValue(c))
									);
						}
					}
					if(count == MAX) {
						count = 0;
						System.out.println(sb.toString());
						sb = new StringBuffer();
					}*/
					try {
						String value = String.valueOf(Bytes.toDouble(CellUtil.cloneValue(c)));
						String time = String.valueOf(ScanUtil.getTimeByCell(c));
						String deviceid = String.valueOf(ScanUtil.getDeviceIdByCell(c));
						String temp = deviceid +
								" : " + time +
								" : " + col +
								" : " + value +
								" : " + count;
						if(value.contains("E")) {
							Integer mi = Integer.valueOf(value.split("E")[1]);
							if(mi > PropUtil.getInt("hbase_number_range") || mi < -PropUtil.getInt("hbase_number_range")) {
								errorRecord.put(deviceid+":"+time, time);
//								System.out.println(temp);
							}
						}
						else if("0.0".equals(value)
								 && (
								 "U1".equals(col)  || "U2".equals(col)  || "U3".equals(col)
								 || "D1".equals(col)  || "D12".equals(col)  || "D13".equals(col)  || "D14".equals(col)
								 || "P4".equals(col)
								 )) {
							errorRecord.put(deviceid+":"+time, time);
//							System.out.println(temp);
						}
						else System.out.println(temp);
						
						/*if(value.charAt(0) == '-') {
							System.out.println(temp);
						}*/
					} catch (Exception e) {
						String value = String.valueOf(Bytes.toFloat(CellUtil.cloneValue(c)));
						String time = String.valueOf(ScanUtil.getTimeByCell(c));
						String deviceid = String.valueOf(ScanUtil.getDeviceIdByCell(c));
						String temp = deviceid +
								" : " + time +
								" : " + col +
								" : " + value +
								" : " + count;
						if(value.contains("E")) {
							Integer mi = Integer.valueOf(value.split("E")[1]);
							if(mi > PropUtil.getInt("hbase_number_range") || mi < -PropUtil.getInt("hbase_number_range")) {
								errorRecord.put(deviceid+":"+time, time);
//								System.out.println(temp);
							}
						}
						else if("0.0".equals(value)
								 && (
										 "U1".equals(col)  || "U2".equals(col)  || "U3".equals(col)
										 || "D1".equals(col)  || "D12".equals(col)  || "D13".equals(col)  || "D14".equals(col)
										 || "P4".equals(col)
										 )) {
							errorRecord.put(deviceid+":"+time, time);
//							System.out.println(temp);
						}
						else System.out.println(temp);
						
						/*if(value.charAt(0) == '-') {
							System.out.println(temp);
						}*/
					}
				}
			}
			
			System.out.println(errorRecord.keySet().size() + ":" + JsonUtil.toJson(errorRecord.keySet()));
			return errorRecord.keySet();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public void scanDemand(short CompanyId, short DeviceId, String startTime, String endTime) throws ParseException, IOException {
		HTableInterface htable =  HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_demandvalue_name"));
				
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
			int count = 0;
			float value = 0f;
			for (Result r : rScaner) {
				for (Cell c : r.rawCells()) {
					String col = Bytes.toString(CellUtil.cloneFamily(c)) + CellUtil.cloneQualifier(c)[0];
					if(col.equals("A1")) {
						float temp = Bytes.toFloat(CellUtil.cloneValue(c));
						if(temp > value) {
							value = temp;
						} else if(temp < value) {
							System.out.println(
									(ScanUtil.getDeviceIdByCell(c)) +
									":" + ScanUtil.getTimeByCell(c) +
									":" + CellUtil.cloneQualifier(c)[0] +
									":" + col +
									":" + Bytes.toFloat(CellUtil.cloneValue(c)) +
									":" + count);
							System.exit(0);
						}
					}
					/*try {
						count++;
						System.out.println(
								(ScanUtil.getDeviceIdByCell(c)) +
								":" + ScanUtil.getTimeByCell(c) +
								":" + CellUtil.cloneQualifier(c)[0] +
								":" + col +
								":" + Bytes.toLong(CellUtil.cloneValue(c)) +
								":" + count);
					} catch (Exception e) {
						try {
							System.out.println(
									(ScanUtil.getDeviceIdByCell(c)) +
									":" + ScanUtil.getTimeByCell(c) +
									":" + CellUtil.cloneQualifier(c)[0] +
									":" + col +
									":" + Bytes.toFloat(CellUtil.cloneValue(c)) +
									":" + count);
						} catch (Exception e2) {
							e.printStackTrace();
						}
					}*/
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
		//degreevalue_TotalEpi=A,4
		//degreevalue_TotalEQind=A,6
		
		/*scan.addColumn(Bytes.toBytes("A"), new byte[]{Byte.valueOf("3")});
		scan.addColumn(Bytes.toBytes("A"), new byte[]{Byte.valueOf("6")});*/
		
		scan.setCaching(100);
		scan.setBatch(2);
		
		try {
			ResultScanner rScaner = htable.getScanner(scan);
			int count = 0;
			for (Result r : rScaner) {
				for (Cell c : r.rawCells()) {
					String col = Bytes.toString(CellUtil.cloneFamily(c)) + CellUtil.cloneQualifier(c)[0];
					try {
						count++;
						System.out.println(
								(ScanUtil.getDeviceIdByCell(c)) +
								":" + ScanUtil.getTimeByCell(c) +
								":" + CellUtil.cloneQualifier(c)[0] +
								":" + col +
								":" + Bytes.toDouble(CellUtil.cloneValue(c)) +
								":" + count);
					} catch (Exception e) {
						try {
							System.out.println(
									(ScanUtil.getDeviceIdByCell(c)) +
									":" + ScanUtil.getTimeByCell(c) +
									":" + CellUtil.cloneQualifier(c)[0] +
									":" + col +
									":" + Bytes.toFloat(CellUtil.cloneValue(c)) +
									":" + count);
						} catch (Exception e2) {
							e.printStackTrace();
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void scanGroup(short CompanyId, short DeviceId, String startTime, String endTime) throws ParseException, IOException {
		HTableInterface htable =  HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_group_name"));
				
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
					String col = Bytes.toString(CellUtil.cloneFamily(c)) + CellUtil.cloneQualifier(c)[0];
					try {
						System.out.println(
								(ScanUtil.getDeviceIdByCell(c)) +
								":" + ScanUtil.getTimeByCell(c) +
								":" + CellUtil.cloneQualifier(c)[0] +
								":" + col +
								":" + Bytes.toDouble(CellUtil.cloneValue(c)));
					} catch (Exception e) {
						try {
							System.out.println(
									(ScanUtil.getDeviceIdByCell(c)) +
									":" + ScanUtil.getTimeByCell(c) +
									":" + CellUtil.cloneQualifier(c)[0] +
									":" + col +
									":" + Bytes.toFloat(CellUtil.cloneValue(c)));
						} catch (Exception e2) {
							e.printStackTrace();
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void scanAlarmdata(short CompanyId, short DeviceId, String startTime, String endTime) throws ParseException, IOException {
		HTableInterface htable =  HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_alarmdata_name"));
				
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
					String col = Bytes.toString(CellUtil.cloneFamily(c)) + CellUtil.cloneQualifier(c)[0];
					try {
						System.out.println(
								(ScanUtil.getDeviceIdByCell(c)) +
								":" + ScanUtil.getTimeByCell(c) +
								":" + CellUtil.cloneQualifier(c)[0] +
								":" + col +
								":" + Bytes.toLong(CellUtil.cloneValue(c)));
					} catch (Exception e) {
						try {
							System.out.println(
									(ScanUtil.getDeviceIdByCell(c)) +
									":" + ScanUtil.getTimeByCell(c) +
									":" + CellUtil.cloneQualifier(c)[0] +
									":" + col +
									":" + Bytes.toFloat(CellUtil.cloneValue(c)));
						} catch (Exception e2) {
							System.out.println(
									(ScanUtil.getDeviceIdByCell(c)) +
									":" + ScanUtil.getTimeByCell(c) +
									":" + CellUtil.cloneQualifier(c)[0] +
									":" + col +
									":" + CellUtil.cloneValue(c)[0]);
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void scanAlarmwave(short CompanyId, short DeviceId, String startTime, String endTime) throws ParseException, IOException {
		HTableInterface htable =  HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_alarmwave_name"));
				
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
					if(qua == 1){
					    
					}
					else if(qua == 4){
						byte[] data = CellUtil.cloneValue(c);
						byte[] temp = new byte[4];
						StringBuffer buf = new StringBuffer();
						for(int i=0; i<4520; ) {
							temp[0] = data[i];
							temp[1] = data[i+1];
							temp[2] = data[i+2];
							temp[3] = data[i+3];
							buf.append(String.valueOf(Bytes.toFloat(temp))+",");
							
							i += 4;
						}
//						System.out.println("ua:"+buf.toString());
					} else if(qua == 5){
						byte[] data = CellUtil.cloneValue(c);
						byte[] temp = new byte[4];
						StringBuffer buf = new StringBuffer();
						for(int i=0; i<4520; ) {
							temp[0] = data[i];
							temp[1] = data[i+1];
							temp[2] = data[i+2];
							temp[3] = data[i+3];
							buf.append(String.valueOf(Bytes.toFloat(temp))+",");
							
							i += 4;
						}
//						System.out.println("ub:"+buf.toString());
					} else if(qua == 6){
						byte[] data = CellUtil.cloneValue(c);
						byte[] temp = new byte[4];
						StringBuffer buf = new StringBuffer();
						for(int i=0; i<4520; ) {
							temp[0] = data[i];
							temp[1] = data[i+1];
							temp[2] = data[i+2];
							temp[3] = data[i+3];
							buf.append(String.valueOf(Bytes.toFloat(temp))+",");
							
							i += 4;
						}
//						System.out.println("uc:"+buf.toString());
					} else if(qua == 7){
						byte[] data = CellUtil.cloneValue(c);
						byte[] temp = new byte[4];
						StringBuffer buf = new StringBuffer();
						for(int i=0; i<4520; ) {
							temp[0] = data[i];
							temp[1] = data[i+1];
							temp[2] = data[i+2];
							temp[3] = data[i+3];
							buf.append(String.valueOf(Bytes.toFloat(temp))+",");
							
							i += 4;
						}
//						System.out.println("ia:"+buf.toString());
					} else if(qua == 8){
						byte[] data = CellUtil.cloneValue(c);
						byte[] temp = new byte[4];
						StringBuffer buf = new StringBuffer();
						for(int i=0; i<4520; ) {
							temp[0] = data[i];
							temp[1] = data[i+1];
							temp[2] = data[i+2];
							temp[3] = data[i+3];
							buf.append(String.valueOf(Bytes.toFloat(temp))+",");
							
							i += 4;
						}
//						System.out.println("ib:"+buf.toString());
					} else if(qua == 9){
						byte[] data = CellUtil.cloneValue(c);
						byte[] temp = new byte[4];
						StringBuffer buf = new StringBuffer();
						for(int i=0; i<4520; ) {
							temp[0] = data[i];
							temp[1] = data[i+1];
							temp[2] = data[i+2];
							temp[3] = data[i+3];
							buf.append(String.valueOf(Bytes.toFloat(temp))+",");
							
							i += 4;
						}
//						System.out.println("ic:"+buf.toString());
					} else {
						try {
							System.out.println(
									(ScanUtil.getDeviceIdByCell(c)) +
									":" + DateUtil.formatToHHMMSS(ScanUtil.getTimeByCell(c)) +
									":" + CellUtil.cloneQualifier(c)[0] +
									":" + Bytes.toLong(CellUtil.cloneValue(c)));
						} catch (Exception e) {
							try {
								System.out.println(
										(ScanUtil.getDeviceIdByCell(c)) +
										":" + DateUtil.formatToHHMMSS(ScanUtil.getTimeByCell(c)) +
										":" + CellUtil.cloneQualifier(c)[0] +
										":" + Bytes.toFloat(CellUtil.cloneValue(c)));
							} catch (Exception e2) {
								System.out.println(
										(ScanUtil.getDeviceIdByCell(c)) +
										":" + DateUtil.formatToHHMMSS(ScanUtil.getTimeByCell(c)) +
										":" + CellUtil.cloneQualifier(c)[0] +
										":" + CellUtil.cloneValue(c)[0]);
							}
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
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
		
//		scan = new Scan();
		
		scan.setCaching(100);
		scan.setBatch(2);
		
		try {
			ResultScanner rScaner = htable.getScanner(scan);
			for (Result r : rScaner) {
				for (Cell c : r.rawCells()) {
					int qua = CellUtil.cloneQualifier(c)[0];
					if(qua == 7/* || qua == 8 || qua == 9 || qua == 10 || qua == 11 || qua == 12*/){
						byte[] data = CellUtil.cloneValue(c);
						System.out.println("data length : " + data.length);
						byte[] temp = new byte[4];
						StringBuffer buf = new StringBuffer();
						for(int i=1; i<1917; ) {
							temp[0] = data[i];
							temp[1] = data[i+1];
							temp[2] = data[i+2];
							temp[3] = data[i+3];
							buf.append(String.valueOf(Bytes.toFloat(temp))+",");
							
							i += 4;
						}
						System.out.println(buf.toString());
					} else {
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
//					System.out.println(ScanUtil.getCompanyIdByCell(c)); System.exit(0);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void addQulitywave() throws IOException {
		Long l1 = new Date().getTime();
		Put put = new Put(Bytes.add(Bytes.toBytes((short)1),
				Bytes.toBytes((short)1001),
				Bytes.toBytes(l1)));
		byte[] FaQualifier = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[0]}, 0,
				Bytes.toBytes(321.34f));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[1]}, 0,
				Bytes.toBytes(new Date().getTime()));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[2]}, 0,
				Bytes.toBytes(220.00531f));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[3]}, 0,
				Bytes.toBytes(252.9796f));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[4]}, 0,
				Bytes.toBytes(220.00531f));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[5]}, 0,
				new byte[]{68});
		String s1 = "-130.25627,-172.85287,-211.19318,-244.33322,-271.45703,-291.89658,-305.1487,-310.88705,-308.9703,-299.44574,-282.54782,-258.69257,-228.46751,-192.61679,-152.02321,-107.686295,-60.69779,-12.214737,36.56898,84.45244,130.25627,172.85287,211.19318,244.33322,271.45703,291.89658,305.1487,310.88705,308.9703,299.44574,282.54782,258.69257,228.46751,192.61679,152.02321,107.686295,60.69779,12.214737,-36.56898,-84.45244,-130.25627,-172.85287,-211.19318,-244.33322,-271.45703,-291.89658,-305.1487,-310.88705,-308.9703,-299.44574,-282.54782,-258.69257,-228.46751,-192.61679,-152.02321,-107.686295,-60.69779,-12.214737,36.56898,84.45244,130.25627,172.85287,211.19318,244.33322,271.45703,291.89658,305.1487,310.88705,308.9703,299.44574,282.54782,258.69257,228.46751,192.61679,152.02321,107.686295,60.69779,12.214737,-36.56898,-84.45244,-130.25627,-172.85287,-211.19318,-244.33322,-271.45703,-291.89658,-305.1487,-310.88705,-308.9703,-299.44574,-282.54782,-258.69257,-228.46751,-192.61679,-152.02321,-107.686295,-60.69779,-12.214737,36.56898,84.45244,130.25627,172.85287,211.19318,244.33322,271.45703,291.89658,305.1487,310.88705,308.9703,299.44574,282.54782,258.69257,228.46751,192.61679,152.02321,107.686295,60.69779,12.214737,-36.56898,-84.45244,-130.25627,-172.85287,-211.19318,-244.33322,-271.45703,-291.89658,-305.1487,-310.88705,-308.9703,-299.44574,-282.54782,-258.69257,-228.46751,-192.61679,-152.02321,-107.686295,-60.69779,-12.214737,36.56898,84.45244,130.25627,172.85287,211.19318,244.33322,271.45703,291.89658,305.1487,310.88705,308.9703,299.44574,282.54782,258.69257,228.46751,192.61679,152.02321,107.686295,60.69779,12.214737,-36.56898,-84.45244,-130.25627,-172.85287,-211.19318,-244.33322,-271.45703,-291.89658,-305.1487,-310.88705,-308.9703,-299.44574,-282.54782,-258.69257,-228.46751,-192.61679,-152.02321,-107.686295,-60.69779,-12.214737,36.56898,84.45244,130.25627,172.85287,211.19318,244.33322,271.45703,291.89658,305.1487,310.88705,308.9703,299.44574,282.54782,258.69257,228.46751,192.61679,152.02321,107.686295,60.69779,12.214737,-36.56898,-84.45244,-130.25627,-172.85287,-211.19318,-244.33322,-271.45703,-291.89658,-305.1487,-310.88705,-308.9703,-299.44574,-282.54782,-258.69257,-228.46751,-192.61679,-152.02321,-107.686295,-60.69779,-12.214737,36.56898,84.45244,130.25627,172.85287,211.19318,244.33322,271.45703,291.89658,305.1487,310.88705,308.9703,299.44574,282.54782,258.69257,228.46751,192.61679,152.02321,107.686295,60.69779,12.214737,-36.56898,-84.45244,-130.25627,-172.85287,-211.19318,-244.33322,-271.45703,-291.89658,-305.1487,-310.88705,-308.9703,-299.44574,-282.54782,-258.69257,-228.46751,-192.61679,-152.02321,-107.686295,-60.69779,-12.214737,36.56898,84.45244,130.25627,172.85287,211.19318,244.33322,271.45703,291.89658,305.1487,310.88705,308.9703,299.44574,282.54782,258.69257,228.46751,192.61679,152.02321,107.686295,60.69779,12.214737,-36.56898,-84.45244,-130.25627,-172.85287,-211.19318,-244.33322,-271.45703,-291.89658,-305.1487,-310.88705,-308.9703,-299.44574,-282.54782,-258.69257,-228.46751,-192.61679,-152.02321,-107.686295,-60.69779,-12.214737,36.56898,84.45244,130.25627,172.85287,211.19318,244.33322,271.45703,291.89658,305.1487,310.88705,308.9703,299.44574,282.54782,258.69257,228.46751,192.61679,152.02321,107.686295,60.69779,12.214737,-36.56898,-84.45244,";
		String arr[] = s1.split(",");
		ByteBuffer buf = ByteBuffer.allocate(320*4);
		for(int i=0;i<320;i++) {
			buf.put(Bytes.toBytes(Float.valueOf(arr[i])));
		}
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[6]}, 0,
				Bytes.toBytes(buf));
		String s2 = "0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,356.65207,356.6517,347.8698,330.52206,305.0359,272.03864,232.34296,186.92624,136.90668,83.516045,28.06898,-28.06898,-83.516045,-136.90651,-186.92606,-232.3427,-272.0384,-305.03537,-330.52145,-347.8692,-356.6511,-356.6511,-347.8692,-330.52145,-305.03537,-272.03796,-232.34236,-186.92572,-136.90633,-83.51587,-28.06898,28.06898,83.51587,136.90616,186.92563,232.3421,272.0377,305.03482,330.52094,347.8686,356.65048,356.65015,347.86826,330.5206,305.03458,272.0375,232.34192,186.92546,136.90607,83.5157,28.068893,-28.068893,-83.5157,-136.9059,-186.92528,-232.34175,-272.03726,-305.03406,-330.52005,-347.86765,-356.64954,-356.64954,-347.86765,-330.52005,-305.03406,-272.03674,-232.34131,-186.92493,-136.90572,-83.515526,-28.068806,28.068806,83.515526,136.90555,186.92476,232.34114,272.03656,305.03345,330.51947,347.86703,356.64893,356.6486,347.86676,330.5192,305.03317,272.0363,232.34088,186.92459,136.90546,83.51535,28.06872,-28.06872,-83.51535,-136.90538,-186.92441,-232.3407,-272.03604,-305.03265,-330.5186,-347.86615,-356.64798,-356.64798,-347.86615,-330.5186,-305.03265,-272.0356,-232.34027,-186.92415,-136.9051,-83.515175,-28.06872,28.06872,83.515175,136.90494,186.92398,232.3401,272.03534,305.03214,330.51807,347.86554,356.64737,356.6471,347.8652,330.51773,305.0319,272.0351,232.33992,186.9238,136.90485,83.514915,28.068632,-28.068632,-83.514915,-136.90477,-186.92363,-232.33966,-272.0349,-305.03137,-330.5172,-347.8646,-356.64642,-356.64642,-347.8646,-330.5172,-305.03137,-272.0344,-232.33931,-186.92328,-136.90451,-83.51483,-28.068546,28.068546,83.51483,136.90442,186.92311,232.33914,272.03412,305.03085,330.5166,347.86398,";
		arr = s2.split(",");
		buf = ByteBuffer.allocate(320*4);
		for(int i=0;i<320;i++) {
			buf.put(Bytes.toBytes(Float.valueOf(arr[i])));
		}
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[7]}, 0,
				Bytes.toBytes(buf));
		String s3 = "-182.87582,-141.24863,-96.14351,-48.670956,0.0,48.670956,96.14351,141.24863,182.87582,219.99992,251.70692,277.21613,295.8993,307.29645,311.12692,307.29645,295.8993,277.21613,251.70692,219.99992,182.87582,141.24863,96.14351,48.670956,0.0,-48.670956,-96.14351,-141.24863,-182.87582,-219.99992,-251.70692,-277.21613,-295.8993,-307.29645,-311.12692,-307.29645,-295.8993,-277.21613,-251.70692,-219.99992,-182.87582,-141.24863,-96.14351,-48.670956,0.0,48.670956,96.14351,141.24863,182.87582,219.99992,251.70692,277.21613,295.8993,307.29645,311.12692,307.29645,295.8993,277.21613,251.70692,219.99992,182.87582,141.24863,96.14351,48.670956,0.0,-48.670956,-96.14351,-141.24863,-182.87582,-219.99992,-251.70692,-277.21613,-295.8993,-307.29645,-311.12692,-307.29645,-295.8993,-277.21613,-251.70692,-219.99992,-182.87582,-141.24863,-96.14351,-48.670956,0.0,48.670956,96.14351,141.24863,182.87582,219.99992,251.70692,277.21613,295.8993,307.29645,311.12692,307.29645,295.8993,277.21613,251.70692,219.99992,182.87582,141.24863,96.14351,48.670956,0.0,-48.670956,-96.14351,-141.24863,-182.87582,-219.99992,-251.70692,-277.21613,-295.8993,-307.29645,-311.12692,-307.29645,-295.8993,-277.21613,-251.70692,-219.99992,-182.87582,-141.24863,-96.14351,-48.670956,0.0,48.670956,96.14351,141.24863,182.87582,219.99992,251.70692,277.21613,295.8993,307.29645,311.12692,307.29645,295.8993,277.21613,251.70692,219.99992,182.87582,141.24863,96.14351,48.670956,0.0,-48.670956,-96.14351,-141.24863,-182.87582,-219.99992,-251.70692,-277.21613,-295.8993,-307.29645,-311.12692,-307.29645,-295.8993,-277.21613,-251.70692,-219.99992,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,";
		arr = s3.split(",");
		buf = ByteBuffer.allocate(320*4);
		for(int i=0;i<320;i++) {
			buf.put(Bytes.toBytes(Float.valueOf(arr[i])));
		}
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[8]}, 0,
				Bytes.toBytes(buf));
		String s4 = "-5.192443,-5.879376,-6.421541,-6.805586,-7.022053,-7.065615,-6.935198,-6.634013,-6.169478,-5.553029,-4.799844,-3.928474,-2.96037,-1.919373,-0.831114,0.277608,1.379495,2.447416,3.455073,4.377655,5.192443,5.879376,6.421541,6.805586,7.022053,7.065615,6.935198,6.634013,6.169478,5.553029,4.799844,3.928474,2.96037,1.919373,0.831114,-0.277608,-1.379495,-2.447416,-3.455073,-4.377655,-5.192443,-5.879376,-6.421541,-6.805586,-7.022053,-7.065615,-6.935198,-6.634013,-6.169478,-5.553029,-4.799844,-3.928474,-2.96037,-1.919373,-0.831114,0.277608,1.379495,2.447416,3.455073,4.377655,5.192443,5.879376,6.421541,6.805586,7.022053,7.065615,6.935198,6.634013,6.169478,5.553029,4.799844,3.928474,2.96037,1.919373,0.831114,-0.277608,-1.379495,-2.447416,-3.455073,-4.377655,-5.192443,-5.879376,-6.421541,-6.805586,-7.022053,-7.065615,-6.935198,-6.634013,-6.169478,-5.553029,-4.799844,-3.928474,-2.96037,-1.919373,-0.831114,0.277608,1.379495,2.447416,3.455073,4.377655,5.192443,5.879376,6.421541,6.805586,7.022053,7.065615,6.935198,6.634013,6.169478,5.553029,4.799844,3.928474,2.96037,1.919373,0.831114,-0.277608,-1.379495,-2.447416,-3.455073,-4.377655,-5.192443,-5.879376,-6.421541,-6.805586,-7.022053,-7.065615,-6.935198,-6.634013,-6.169478,-5.553029,-4.799844,-3.928474,-2.96037,-1.919373,-0.831114,0.277608,1.379495,2.447416,3.455073,4.377655,5.192443,5.879376,6.421541,6.805586,7.022053,7.065615,6.935198,6.634013,6.169478,5.553029,4.799844,3.928474,2.96037,1.919373,0.831114,-0.277608,-1.379495,-2.447416,-3.455073,-4.377655,-5.192443,-5.879376,-6.421541,-6.805586,-7.022053,-7.065615,-6.935198,-6.634013,-6.169478,-5.553029,-4.799844,-3.928474,-2.96037,-1.919373,-0.831114,0.277608,1.379495,2.447416,3.455073,4.377655,5.192443,5.879376,6.421541,6.805586,7.022053,7.065615,6.935198,6.634013,6.169478,5.553029,4.799844,3.928474,2.96037,1.919373,0.831114,-0.277608,-1.379495,-2.447416,-3.455073,-4.377655,-5.192443,-5.879376,-6.421541,-6.805586,-7.022053,-7.065615,-6.935198,-6.634013,-6.169478,-5.553029,-4.799844,-3.928474,-2.96037,-1.919373,-0.831114,0.277608,1.379495,2.447416,3.455073,4.377655,5.192443,5.879376,6.421541,6.805586,7.022053,7.065615,6.935198,6.634013,6.169478,5.553029,4.799844,3.928474,2.96037,1.919373,0.831114,-0.277608,-1.379495,-2.447416,-3.455073,-4.377655,-5.192443,-5.879376,-6.421541,-6.805586,-7.022053,-7.065615,-6.935198,-6.634013,-6.169478,-5.553029,-4.799844,-3.928474,-2.96037,-1.919373,-0.831114,0.277608,1.379495,2.447416,3.455073,4.377655,5.192443,5.879376,6.421541,6.805586,7.022053,7.065615,6.935198,6.634013,6.169478,5.553029,4.799844,3.928474,2.96037,1.919373,0.831114,-0.277608,-1.379495,-2.447416,-3.455073,-4.377655,-5.192443,-5.879376,-6.421541,-6.805586,-7.022053,-7.065615,-6.935198,-6.634013,-6.169478,-5.553029,-4.799844,-3.928474,-2.96037,-1.919373,-0.831114,0.277608,1.379495,2.447416,3.455073,4.377655,5.192443,5.879376,6.421541,6.805586,7.022053,7.065615,6.935198,6.634013,6.169478,5.553029,4.799844,3.928474,2.96037,1.919373,0.831114,-0.277608,-1.379495,-2.447416,-3.455073,-4.377655,";
		arr = s4.split(",");
		buf = ByteBuffer.allocate(320*4);
		for(int i=0;i<320;i++) {
			buf.put(Bytes.toBytes(Float.valueOf(arr[i])));
		}
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[9]}, 0,
				Bytes.toBytes(buf));
		String s5 = "0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,13.449968,12.600733,11.441228,10.0,8.312537,6.420392,4.370157,2.212315,0.0,-2.212315,-4.370157,-6.420392,-8.312537,-10.0,-11.441228,-12.600733,-13.449968,-13.968021,-14.142135,-13.968021,-13.449968,-12.600733,-11.441228,-10.0,-8.312537,-6.420392,-4.370157,-2.212315,0.0,2.212315,4.370157,6.420392,8.312537,10.0,11.441228,12.600733,13.449968,13.968021,14.142135,13.968021,13.449968,12.600733,11.441228,10.0,8.312537,6.420392,4.370157,2.212315,0.0,-2.212315,-4.370157,-6.420392,-8.312537,-10.0,-11.441228,-12.600733,-13.449968,-13.968021,-14.142135,-13.968021,-13.449968,-12.600733,-11.441228,-10.0,-8.312537,-6.420392,-4.370157,-2.212315,0.0,2.212315,4.370157,6.420392,8.312537,10.0,11.441228,12.600733,13.449968,13.968021,14.142135,13.968021,13.449968,12.600733,11.441228,10.0,8.312537,6.420392,4.370157,2.212315,0.0,-2.212315,-4.370157,-6.420392,-8.312537,-10.0,-11.441228,-12.600733,-13.449968,-13.968021,-14.142135,-13.968021,-13.449968,-12.600733,-11.441228,-10.0,-8.312537,-6.420392,-4.370157,-2.212315,0.0,2.212315,4.370157,6.420392,8.312537,10.0,11.441228,12.600733,13.449968,13.968021,14.142135,13.968021,13.449968,12.600733,11.441228,10.0,8.312537,6.420392,4.370157,2.212315,0.0,-2.212315,-4.370157,-6.420392,-8.312537,-10.0,-11.441228,-12.600733,-13.449968,-13.968021,-14.142135,-13.968021,-13.449968,-12.600733,-11.441228,-10.0,-8.312537,-6.420392,-4.370157,-2.212315,0.0,2.212315,4.370157,6.420392,8.312537,10.0,11.441228,12.600733,13.449968,13.968021,14.142135,13.968021,";
		arr = s5.split(",");
		buf = ByteBuffer.allocate(320*4);
		for(int i=0;i<320;i++) {
			buf.put(Bytes.toBytes(Float.valueOf(arr[i])));
		}
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[10]}, 0,
				Bytes.toBytes(buf));
		String s6 = "-3.301415,-1.109578,1.109578,3.301415,5.411959,7.389243,9.184581,10.753762,12.058152,13.065628,13.751384,14.098537,14.098537,13.751384,13.065628,12.058152,10.753762,9.184581,7.389243,5.411959,3.301415,1.109578,-1.109578,-3.301415,-5.411959,-7.389243,-9.184581,-10.753762,-12.058152,-13.065628,-13.751384,-14.098537,-14.098537,-13.751384,-13.065628,-12.058152,-10.753762,-9.184581,-7.389243,-5.411959,-3.301415,-1.109578,1.109578,3.301415,5.411959,7.389243,9.184581,10.753762,12.058152,13.065628,13.751384,14.098537,14.098537,13.751384,13.065628,12.058152,10.753762,9.184581,7.389243,5.411959,3.301415,1.109578,-1.109578,-3.301415,-5.411959,-7.389243,-9.184581,-10.753762,-12.058152,-13.065628,-13.751384,-14.098537,-14.098537,-13.751384,-13.065628,-12.058152,-10.753762,-9.184581,-7.389243,-5.411959,-3.301415,-1.109578,1.109578,3.301415,5.411959,7.389243,9.184581,10.753762,12.058152,13.065628,13.751384,14.098537,14.098537,13.751384,13.065628,12.058152,10.753762,9.184581,7.389243,5.411959,3.301415,1.109578,-1.109578,-3.301415,-5.411959,-7.389243,-9.184581,-10.753762,-12.058152,-13.065628,-13.751384,-14.098537,-14.098537,-13.751384,-13.065628,-12.058152,-10.753762,-9.184581,-7.389243,-5.411959,-3.301415,-1.109578,1.109578,3.301415,5.411959,7.389243,9.184581,10.753762,12.058152,13.065628,13.751384,14.098537,14.098537,13.751384,13.065628,12.058152,10.753762,9.184581,7.389243,5.411959,3.301415,1.109578,-1.109578,-3.301415,-5.411959,-7.389243,-9.184581,-10.753762,-12.058152,-13.065628,-13.751384,-14.098537,-14.098537,-13.751384,-13.065628,-12.058152,-10.753762,-9.184581,-7.389243,-5.411959,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,";
		arr = s6.split(",");
		buf = ByteBuffer.allocate(320*4);
		for(int i=0;i<320;i++) {
			buf.put(Bytes.toBytes(Float.valueOf(arr[i])));
		}
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[11]}, 0,
				Bytes.toBytes(buf));
		HTableInterface htable = HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_qualitywave_name"));
		htable.put(put);
		htable.flushCommits();
		System.out.println(l1);
	}

}

