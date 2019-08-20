package com.nikey.fix;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;

import com.nikey.hbase.CommerrHTableMapper;
import com.nikey.hbase.HbaseTablePool;
import com.nikey.util.DateUtil;

/**
 * @author Jayzee
 * @date 2016年10月26日 上午10:14:08
 * 
 * 用途：
 *	设置权值为4096的曲线断点到云主机的monitordata表
 *
 * 输入：
 *  deviceIDs：以逗号分隔的字符串，标识监测点编号
 *  breakTime：yyyy-MM-dd HH:MM:SS格式的字符串，标识曲线断点
 *  
 * 注意：
 *	请确保config.properties配置的华为云主机的公网IP
 *
 * 结果判断：
 *	若正常退出表示加入断点成功，抛出异常表示加入断点失败
 */
public class MonitordataBreakPoint {
	
	public static void main(String[] args) {
		String deviceIDs = "5001,5002,5003,5004,5005,5006,5007,5008,5009,5010";
		String breakTime = "2016-10-07 11:00:29";
		
		MonitordataBreakPoint breakPoint = new MonitordataBreakPoint();
		try {
			HTableInterface htable = HbaseTablePool.instance().getHtable("monitordata");
			breakPoint.breakPoint(deviceIDs, breakTime, htable, new CommerrHTableMapper());
			htable.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.exit(0);
		
	}
	
	public void breakPoint(String deviceIDs, String breakTime, HTableInterface htable, CommerrHTableMapper mapper) {
		String [] strArr = deviceIDs.split(",");
		
		List<Put> puts = new ArrayList<Put>();
		for(String deviceID : strArr) {
			Put put = mapper.getPutForMonitordataFix((short) (Short.valueOf(deviceID)/1000), Short.valueOf(deviceID), DateUtil.parseHHMMSSToDate(breakTime).getTime());
			puts.add(put);
		}
		
		if(puts.size() > 0) {
			try {
				htable.put(puts);
				htable.flushCommits();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}
