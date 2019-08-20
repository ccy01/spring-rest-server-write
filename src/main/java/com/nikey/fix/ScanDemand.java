package com.nikey.fix;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.nikey.hbase.HbaseTablePool;
import com.nikey.util.DateUtil;

/**
 * @author Jayzee
 * @date 2016年7月18日 下午3:19:18
 * 
 * 查询并打印需量记录
 */
public class ScanDemand {
	
	public static void main(String[] args) {
		ScanDemand sd = new ScanDemand();
		
		short CompanyID = 5;
		short DeviceID = 5001;
		String StartTime = "2016-06-17 00:00:00";
		String EndTime = "2016-07-12 00:00:00";
		String ColumnFamily = "P";
		String Qulifier = "28";
		
		try {
			HTableInterface htable = HbaseTablePool.instance().getHtable("monitordata");
			sd.scanDemand(CompanyID, DeviceID, StartTime, EndTime, ColumnFamily, Qulifier, htable);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void scanDemand(short CompanyID, short DeviceID, String StartTime,
			String EndTime, String ColumnFamily, String Qulifier, HTableInterface htable) {
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
		scan.setFilter(new PageFilter(100l));
		try {
			ResultScanner scanner = htable.getScanner(scan);
			for (Result result : scanner) {
				for (Cell cell : result.rawCells()) {
					if(cell.getTimestamp() != 4096l) {
						byte qualifier = CellUtil.cloneQualifier(cell)[0];
						switch (qualifier) {
						case 24:
							System.out.println(Bytes.toFloat(CellUtil.cloneValue(cell)));
							break;
						case 28:
							System.out.println(DateUtil.formatToHHMMSS(Bytes.toLong(CellUtil.cloneValue(cell))));
							break;
						}
					} 
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
