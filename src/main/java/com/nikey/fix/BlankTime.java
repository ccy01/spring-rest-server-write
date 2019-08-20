package com.nikey.fix;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.nikey.hbase.HbaseTablePool;
import com.nikey.util.DateUtil;
import com.nikey.util.ScanUtil;

/**
 * @author Jayzee
 * @date 2016年7月11日 上午11:09:16
 * 
 * 扫描点与点之前超过10分钟的时间区间，
 * 输入：
 * 		String tableNames = "monitordata"; // hbase表名
		String columnFamilys = "P";        // columFamily
		short CompanyId = 5;               // 公司编号
		short StartDeviceId = 5009;        // 开始监测点
		short EndDeviceId = 5009;          // 结束监测点
		byte qualifier = 22;               // 列族编号
		long sub = 60 * 1000 * 5;          // 毫秒值（多长时间算超时）
		String startStr = "2016-08-11 00:00:00";
		String endStr = "2016-08-12 00:00:00";
	输出：
		开始时间 ： 2016-08-11 00:41:35  结束时间 ： 2016-08-11 00:47:53  间隔时间（秒） ：378
		开始时间 ： 2016-08-11 00:56:02  结束时间 ： 2016-08-11 01:07:15  间隔时间（秒） ：673
 */
public class BlankTime {
	
	public static void main(String[] args) {
		String tableNames = "monitordata";
		String columnFamilys = "U";
		short CompanyId = 5;
		short StartDeviceId = 5001;
		short EndDeviceId = 5009;
		byte qualifier = 1;
		long sub = 60 * 1000 * 30; // 30min
		String startStr = "2016-08-29 00:00:00";
		String endStr = "2016-09-06 00:00:00";
		long startTime = DateUtil.parseHHMMSSToDate(startStr).getTime();
		long endTime = DateUtil.parseHHMMSSToDate(endStr).getTime();
		try {
			HTableInterface htable = HbaseTablePool.instance().getHtable(tableNames);
			scanTable(tableNames, CompanyId, StartDeviceId, EndDeviceId,
					startTime, endTime, htable, columnFamilys, qualifier, sub);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	static public void scanTable(String tableNames, short CompanyId,
			short StartDeviceId, short EndDeviceId, long startTime,
			long endTime, HTableInterface htable, String columnFamily, 
			byte qualifier, long sub) {
		if (StartDeviceId != 0 && EndDeviceId != 0) {
			for (short DeviceID = StartDeviceId; DeviceID <= EndDeviceId; DeviceID++) {
				byte[] startKey = Bytes.add(Bytes.toBytes(CompanyId),
						Bytes.toBytes(DeviceID), Bytes.toBytes(startTime));
				byte[] endKey = Bytes.add(Bytes.toBytes(CompanyId),
						Bytes.toBytes(DeviceID), Bytes.toBytes(endTime));
				Scan scan = new Scan(startKey, endKey);
				scan.addColumn(Bytes.toBytes(columnFamily),
						new byte[] { qualifier });
				try {
					Long firstGoodTime = 0l, secondGoodTime = 0l;
					ResultScanner rScaner = htable.getScanner(scan);
					for (Result r : rScaner) {
						for (Cell cell : r.rawCells()) {
							long InsertTime = ScanUtil.getTimeByCell(cell);
							if (cell.getTimestamp() != 4096l) {
								if (firstGoodTime == 0) {
									firstGoodTime = InsertTime;
									System.out.println("DeviceID is "
											+ DeviceID
											+ ", firstGoodTime is "
											+ firstGoodTime);
								} else {
									if (secondGoodTime == 0) {
										secondGoodTime = InsertTime;
									} else {
										firstGoodTime = secondGoodTime;
										secondGoodTime = InsertTime;
									}
									// 两个正常点相距时间超过10min
									if ((secondGoodTime - firstGoodTime) > sub) {
										System.out
										.println("开始时间 ： "
											+ DateUtil
													.formatToHHMMSS(firstGoodTime)
											+ "  结束时间 ： "
											+ DateUtil
													.formatToHHMMSS(secondGoodTime)
											+ "  间隔时间（秒） ："
											+ ((secondGoodTime - firstGoodTime) / 1000));
									}
								}
							}
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			
			}
		}
	}

	static public void terminalSettingsBackup2(String tableName,
			String columnFamily, Short deviceid, String backupstr) {
		File directory = new File("");// 设定为当前文件夹
		try {
			String folder = directory.getAbsolutePath();
			String path = folder + File.separator + "backup2" + File.separator
					+ tableName + File.separator + columnFamily
					+ File.separator + deviceid / 1000 + ".txt";

			File file = new File(path);
			if (!file.getParentFile().exists()) {
				file.getParentFile().mkdirs();
			}
			if (!file.exists()) {
				file.createNewFile();
			}
			if (file.exists()) {
				FileWriter writer = new FileWriter(file, true);
				writer.write(backupstr);
				writer.flush();
				writer.close();

			}
		} catch (Exception e) {
		}
	}
}
