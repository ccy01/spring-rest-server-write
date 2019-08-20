package com.nikey.fix;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
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

/**
 * @author Jayzee
 * @date 2016年7月11日 上午10:28:40
 * 
 *  通过StartTime和EndTime获取空白超过MAX时间（秒）以上的[DeviceID，开始时间，结束时间，持续时间]
 */
public class ScanBlankByRowkey {
	
	String SQL = "select msg_id from messagedata_h where device_id = %d and insert_time > %d and insert_time < %d and type = 1;";
	CommerrHTableMapper commerr = new CommerrHTableMapper();

	public static void main(String[] args) {
		
		ScanBlankByRowkey blank = new ScanBlankByRowkey();
		
		short CompanyID = 5;
		short DeviceID = 5001;
		String StartTime = "2014-01-01 00:00:00";
		String EndTime = "2014-12-12 00:00:00";
		String ColumnFamily = "U";
		String Qulifier = "1";
		long MAX = 600000l;
		try {
			HTableInterface htable = HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_monitordata_name"));
			for(; DeviceID <= 5010; DeviceID ++) {
				blank.scanBlank(CompanyID, DeviceID, StartTime, EndTime, ColumnFamily, Qulifier, MAX, htable);
			}
			/*HTableInterface htable = HbaseTablePool.instance().getHtable("temperature");
			blank.scanBlankForTemperature(CompanyID, (short) 5901, StartTime, EndTime, "A", "4", MAX, htable);
			blank.scanBlankForTemperature(CompanyID, (short) 5902, StartTime, EndTime, "A", "5", MAX, htable);*/
			htable.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void scanBlank(short CompanyID, short DeviceID, String StartTime,
			String EndTime, String ColumnFamily, String Qulifier, long MAX,
			HTableInterface htable) {
		Scan scan = new Scan();
		try {
			scan.setStartRow(Bytes.add(Bytes.toBytes(CompanyID),
					Bytes.toBytes(DeviceID), Bytes.toBytes(DateUtil.parseHHMMSSToDate(StartTime).getTime())));
			scan.setStopRow(Bytes.add(Bytes.toBytes(CompanyID),
					Bytes.toBytes(DeviceID), Bytes.toBytes(DateUtil.parseHHMMSSToDate(EndTime).getTime() + 1))); // 右开区间
		} catch (Exception e) {
			scan.setStartRow(Bytes.add(Bytes.toBytes(CompanyID),
					Bytes.toBytes(DeviceID), Bytes.toBytes(Long.valueOf(StartTime))));
			scan.setStopRow(Bytes.add(Bytes.toBytes(CompanyID),
					Bytes.toBytes(DeviceID), Bytes.toBytes(Long.valueOf(EndTime) + 1))); // 右开区间
		}
		try {
			scan.setTimeRange(0l, Long.MAX_VALUE);
		} catch (IOException e) {
			e.printStackTrace();
		}
		scan.addColumn(Bytes.toBytes(ColumnFamily), new byte[] { Byte.valueOf(Qulifier) });
		List<Delete> dels = new ArrayList<Delete>();
		List<Put> puts = new ArrayList<Put>();
		try {
			ResultScanner scanner = htable.getScanner(scan);
			long minTime = 0l, maxTime = 0l;
			for (Result result : scanner) {
				for (Cell cell : result.rawCells()) {
					long insertTime = ScanUtil.getTimeByCell(cell);
					if(cell.getTimestamp() != 4096l) {
						if(minTime == 0l) {
							minTime = insertTime;
						} else {
							if(maxTime == 0l) {
								maxTime = insertTime; // BINGO
							} else {
								minTime = maxTime;
								maxTime = insertTime; // BINGO
							}
							long sub = (maxTime - minTime);
							if(sub > MAX) {
								System.out.println(String.format("%d|%s|%s|%d", DeviceID, DateUtil.formatToHHMMSS(minTime),
										DateUtil.formatToHHMMSS(maxTime), sub/1000));
								
//								System.out.println(
//										String.format(SQL, DeviceID, minTime/1000,
//												maxTime/1000, (maxTime - minTime)/1000));
								
								if(sub > 1500000l) { // 断线30分钟，1500秒
									Put put = commerr.getPutForMonitordataFix(CompanyID, DeviceID, minTime + 1); // minTime偏置1毫秒
									puts.add(put);
								}
							}
						}
					} else {
						dels.add(new Delete(Bytes.add(Bytes.toBytes(CompanyID),
								Bytes.toBytes(DeviceID), Bytes.toBytes(insertTime))));
					}
				}
			}
			System.out.println(String.format("%d dels %d", DeviceID, dels.size()));
			System.out.println(String.format("%d puts %d", DeviceID, puts.size()));
//			if(dels.size() != 0) htable.delete(dels);
//			if(puts.size() != 0) htable.put(puts);
//			htable.flushCommits();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void scanBlankForTemperature(short CompanyID, short DeviceID, String StartTime,
			String EndTime, String ColumnFamily, String Qulifier, long MAX,
			HTableInterface htable) {
		Scan scan = new Scan();
		try {
			scan.setStartRow(Bytes.add(Bytes.toBytes(CompanyID),
					Bytes.toBytes(DeviceID), Bytes.toBytes(DateUtil.parseHHMMSSToDate(StartTime).getTime())));
			scan.setStopRow(Bytes.add(Bytes.toBytes(CompanyID),
					Bytes.toBytes(DeviceID), Bytes.toBytes(DateUtil.parseHHMMSSToDate(EndTime).getTime() + 1))); // 右开区间
		} catch (Exception e) {
			scan.setStartRow(Bytes.add(Bytes.toBytes(CompanyID),
					Bytes.toBytes(DeviceID), Bytes.toBytes(Long.valueOf(StartTime))));
			scan.setStopRow(Bytes.add(Bytes.toBytes(CompanyID),
					Bytes.toBytes(DeviceID), Bytes.toBytes(Long.valueOf(EndTime) + 1))); // 右开区间
		}
		try {
			scan.setTimeRange(0l, Long.MAX_VALUE);
		} catch (IOException e) {
			e.printStackTrace();
		}
		scan.addColumn(Bytes.toBytes(ColumnFamily), new byte[] { Byte.valueOf(Qulifier) });
		List<Delete> dels = new ArrayList<Delete>();
		List<Put> puts = new ArrayList<Put>();
		try {
			ResultScanner scanner = htable.getScanner(scan);
			long minTime = 0l, maxTime = 0l;
			for (Result result : scanner) {
				for (Cell cell : result.rawCells()) {
					long insertTime = ScanUtil.getTimeByCell(cell);
					if(cell.getTimestamp() != 4096l) {
						if(minTime == 0l) {
							minTime = insertTime;
						} else {
							if(maxTime == 0l) {
								maxTime = insertTime; // BINGO
							} else {
								minTime = maxTime;
								maxTime = insertTime; // BINGO
							}
							long sub = (maxTime - minTime);
							if(sub > MAX) {
								System.out.println(String.format("%d|%s|%s|%d", DeviceID, DateUtil.formatToHHMMSS(minTime),
										DateUtil.formatToHHMMSS(maxTime), sub/1000));
								
//								System.out.println(
//										String.format(SQL, DeviceID, minTime/1000,
//												maxTime/1000, (maxTime - minTime)/1000));
								
//								if(sub > 1500000l) { // 断线30分钟，1500秒
//									Put put = commerr.getPutForMonitordataFix(CompanyID, DeviceID, minTime + 1); // minTime偏置1毫秒
//									puts.add(put);
//								}
							}
						}
					} else {
						dels.add(new Delete(Bytes.add(Bytes.toBytes(CompanyID),
								Bytes.toBytes(DeviceID), Bytes.toBytes(insertTime))));
					}
				}
			}
			System.out.println(String.format("%d dels %d", DeviceID, dels.size()));
//			System.out.println(String.format("%d puts %d", DeviceID, puts.size()));
			if(dels.size() != 0) htable.delete(dels);
//			if(puts.size() != 0) htable.put(puts);
			htable.flushCommits();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
