package com.nikey.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.nikey.hbase.CommerrHTableMapper;
import com.nikey.hbase.HbaseTablePool;
import com.nikey.hbase.MonitordataHTableMapper;
import com.nikey.util.DateUtil;
import com.nikey.util.ScanUtil;

public class MonitordataFix {
	
	short companyId = 1, startDevice = 1001, endDevice = 1024;
	byte searchType = 0;
	long offsetTime = 1000 * 60 * 10; // 数据偏移10分钟
	MonitordataHTableMapper mapper = new MonitordataHTableMapper();
	CommerrHTableMapper helper = new CommerrHTableMapper();
	
	public static void main(String[] args) {
		MonitordataFix fix = new MonitordataFix();
		fix.searchType = 0;
		fix.hourAndDayMonitordataFix("2015-01-01 00:00:00", "2015-04-14 00:00:00", "monitordata_hour");
		fix.searchType = 1;
		fix.hourAndDayMonitordataFix("2015-01-01 00:00:00", "2015-04-14 00:00:00", "monitordata_day");
		fix.monitordataFix("2015-01-01 00:00:00", "2015-04-14 00:00:00");
		System.out.println("happy ending ...");
	}
	
	public void monitordataFix(String starttime, String endtime) {
		try {
			long startTime = DateUtil.parseHHMMSSToDate(starttime).getTime();
			long endTime = DateUtil.parseHHMMSSToDate(endtime).getTime();
			
			for(short DeviceId = startDevice; DeviceId <= endDevice; DeviceId ++) {
				Scan scan = new Scan();
		        scan.setStartRow(Bytes.add(Bytes.toBytes(companyId),
		                Bytes.toBytes(DeviceId), Bytes.toBytes(startTime)));
		        scan.setStopRow(Bytes.add(Bytes.toBytes(companyId),
		                Bytes.toBytes(DeviceId), Bytes.toBytes(endTime)));
		        scan.addColumn(Bytes.toBytes("U"), new byte[]{(byte) 1});
		        
		        Long firstTime = 0l, lastTime = 0l;
		        Long firstTimeStamp = 0l, lastTimeStamp = 0l;
		        ResultScanner scanner = mapper.getResultScannerWithParameterMap(scan);
		        // init
		        int counter = 0;
		        for(Result result : scanner) {
		        	for(Cell cell : result.rawCells()) {
		        		if(firstTime == 0l) {
		        			firstTime = ScanUtil.getTimeByCell(cell);
		        			firstTimeStamp = cell.getTimestamp();
		        		} else {
		        			lastTime = ScanUtil.getTimeByCell(cell);
		        			lastTimeStamp = cell.getTimestamp();
		        		}
		        		break;
		        	}
		        	counter++;
		        	if(counter == 2) {
		        		break;
		        	}
		        }
		        for(Result result : scanner) {
		        	// timestamp 不为4096 && 间隔offsetTime分钟没有数据
		        	if(firstTimeStamp != 4096l && lastTimeStamp != 4096l && (lastTime - firstTime > offsetTime)) {
		        		// 插空数据
		        		long time = lastTime - 1;
		        		System.out.println("data missing : " + DeviceId + "," + DateUtil.formatToHHMMSS(time));
		        		Put put = helper.getPutForMonitordataFix(companyId, DeviceId, time);
		        		List<Put> puts = new ArrayList<Put>();
		        		puts.add(put);
		        		mapper.put(puts);
		        	} 
		        	firstTime = lastTime;
		        	firstTimeStamp = lastTimeStamp;
		        	for(Cell cell : result.rawCells()) {
		        		lastTime = ScanUtil.getTimeByCell(cell);
	        			lastTimeStamp = cell.getTimestamp();
	        			break;
		        	}
		        }
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void hourAndDayMonitordataFix(String starttime, String endtime, String tableName) {
		try {
			HTableInterface htable = HbaseTablePool.instance().getHtable(tableName);
			long startTime = DateUtil.parseHHMMSSToDate(starttime).getTime();
			long endTime = DateUtil.parseHHMMSSToDate(endtime).getTime();
			long sub = 0;
			if(searchType == 0) {
				// hour
				sub = 1000 * 60 * 60;
			} else {
				// day
				sub = 1000 * 60 * 60 * 24;
			}
			
			for(short DeviceId = startDevice; DeviceId <= endDevice; DeviceId ++) {
				for(long time = startTime; time <= endTime; time += sub) {
					Get get = new Get(Bytes.add(Bytes.toBytes(companyId),
							Bytes.toBytes(DeviceId),
							Bytes.toBytes(time)
						));
					try {
						Result result = htable.get(get);
						boolean isEmpty = true;
						if(result.rawCells().length > 0) {
							isEmpty = false;
						}
						if(isEmpty) {
							System.out.println("data missing : " + DeviceId + "," + DateUtil.formatToHHMMSS(time));
							Put put = getPut(DeviceId, time);
							htable.put(put);
						}
						
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public Put getPut(short DeviceId, long time) {
		Put put = new Put(Bytes.add(Bytes.toBytes(companyId),
				Bytes.toBytes(DeviceId),
				Bytes.toBytes(time)
			));
		// only P0 is fixed
		put.add(Bytes.toBytes("P"), new byte[]{(byte) 58}, 4096l,
				Bytes.toBytes(0f));
		return put;
	}

}
