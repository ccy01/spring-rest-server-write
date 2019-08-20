package com.nikey.fix;

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
import com.nikey.util.ScanUtil;

/**
 * @author Jayzee
 * @date 2016年3月7日 下午5:12:32
 * 
 * hbase之monitordata表
 * 由于通信程序误判，产生了大量的曲线断点，此程序用于去除无用的曲线断点
 * 判断条件：
 * 1. 两个正常点相距时间不超过10min，将区间内的曲线断点删除；
 * 2. 两个正常点相距时间超过10min，区间内若无曲线断点则添加；
 */
public class MonitordataBreakFix {
	
	public static void main(String[] args) {
		MonitordataBreakFix fix = new MonitordataBreakFix();
		fix.monitordataBreakFix((short) 4, (short) 4001, (short) 4001, "2016-03-08 00:00:00", "2016-03-09 00:00:00");
	}
	
	public void monitordataBreakFix(short companyId, short startDevice, short endDevice, String starttime, String endtime) {
		try {
			HTableInterface htable = HbaseTablePool.instance().getHtable("monitordata");
			CommerrHTableMapper mapper = new CommerrHTableMapper();
			long startTime = DateUtil.parseHHMMSSToDate(starttime).getTime();
			long endTime = DateUtil.parseHHMMSSToDate(endtime).getTime();
			for(short DeviceId = startDevice; DeviceId <= endDevice; DeviceId ++) {
				Scan scan = new Scan();
		        scan.setStartRow(Bytes.add(Bytes.toBytes(companyId),
		                Bytes.toBytes(DeviceId), Bytes.toBytes(startTime)));
		        scan.setStopRow(Bytes.add(Bytes.toBytes(companyId),
		                Bytes.toBytes(DeviceId), Bytes.toBytes(endTime)));
		        // 扫描Ua作为依据
		        scan.addColumn(Bytes.toBytes("U"), new byte[]{(byte) 1});
		        ResultScanner scanner = htable.getScanner(scan);
		        
		        Long firstGoodTime = 0l, secondGoodTime = 0l;
		        List<Delete> dels = new ArrayList<Delete>();
		        List<Put> puts = new ArrayList<Put>();
		        List<Long> dels2print = new ArrayList<Long>();
		        List<Long> puts2print = new ArrayList<Long>();
		        List<Long> longs = new ArrayList<Long>();
		        long sub = 600000l + 60000; // 600s + 60s
		        
		        for(Result result : scanner) {
		        	for(Cell cell : result.rawCells()) {
		        		long InsertTime = ScanUtil.getTimeByCell(cell);
		        		System.out.println(String.format("[%d]-[%s]-[%d]", DeviceId, DateUtil.formatToHHMMSS(InsertTime), cell.getTimestamp()));
		        		if(cell.getTimestamp() != 4096l) {
		        			if(firstGoodTime == 0) {
		        				firstGoodTime = InsertTime;
		        			} else {
		        				if(secondGoodTime == 0) {
			        				secondGoodTime = InsertTime;
			        			} else {
			        				firstGoodTime = secondGoodTime;
			        				secondGoodTime = InsertTime;
			        			}
		        				// 两个正常点相距时间不超过10min，将区间内的曲线断点删除
		        				if((secondGoodTime - firstGoodTime) <= sub) {
		        					for(Long tmp : longs) {
		        						dels.add(new Delete(Bytes.add(Bytes.toBytes(companyId),
		        								Bytes.toBytes(DeviceId), Bytes.toBytes(tmp))));
		        						dels2print.add(tmp);
		        					}
		        				} else {
		        					// 两个正常点相距时间超过10min，区间内若无曲线断点则添加
		        					if(longs.size() == 0) {
		        						puts.add(mapper.getPutForMonitordataFix(companyId, DeviceId, secondGoodTime - 1000));
		        						puts2print.add(secondGoodTime - 1000);
		        					}
		        				}
		        				longs.clear();
		        			}
		        		} else {
		        			longs.add(InsertTime);
		        		}
		        	}
		        }
				
				for(Long tmp : dels2print) {
					System.out.println(String.format("[Delete]-[%d]-[%s]", DeviceId, DateUtil.formatToHHMMSS(tmp)));
				}
				for(Long tmp : puts2print) {
					System.out.println(String.format("[Put]-[%d]-[%s]", DeviceId, DateUtil.formatToHHMMSS(tmp)));
				}
				
				htable.delete(dels);
				htable.put(puts);
				htable.flushCommits();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
