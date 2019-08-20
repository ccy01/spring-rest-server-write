package com.nikey.hbase;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nikey.util.NumberUtil;
import com.nikey.util.PropUtil;

/**
 * 
 * @author ouyang
 * @date 2014-12-04
 * @descirption temperatureHTableMapper
 *
 */
public class TemperatureHTableMapper implements HTableMapper{
	
	/**
	 * slfj
	 */
	Logger logger = LoggerFactory.getLogger(getClass());
	
	private ThreadLocal<HTableInterface> htables = new ThreadLocal<HTableInterface>();

	@Override
	public List<Put> convertParameterMapToPut(Map<String, String[]> request) {
		short CompanyId = 0;
		short DeviceId = 0;
		long InsertTime = 0l;
		try {
			CompanyId = NumberUtil.Short_valueOf(request, "CompanyId");
			DeviceId = NumberUtil.Short_valueOf(request, "DeviceId");
			InsertTime = NumberUtil.Long_valueOf(request, "InsertTime");
			InsertTime = InsertTime * 1000l; // 放大为毫秒值
		} catch (Exception e) {
			// 数据畸变，返回null
			e.printStackTrace();
			return null;
		}
		
		Put put = new Put(Bytes.add(Bytes.toBytes(CompanyId),
				Bytes.toBytes(DeviceId),
				Bytes.toBytes(InsertTime)));
		
		int TimeFans = Integer.valueOf(request.get("TimeFans")[0]);
		float TemperatureA = Float.valueOf(request.get("TemperatureA")[0]);
		float TemperatureB = Float.valueOf(request.get("TemperatureB")[0]);
		float TemperatureC = Float.valueOf(request.get("TemperatureC")[0]);
		float TemperatureI = Float.valueOf(request.get("TemperatureI")[0]);
		byte  IsAOk  = Byte.valueOf(request.get("IsAOk")[0]); 
		byte  IsBOk  = Byte.valueOf(request.get("IsBOk")[0]);
		byte  IsCOk  = Byte.valueOf(request.get("IsCOk")[0]); 
		byte  IsFansOpen  = Byte.valueOf(request.get("IsFansOpen")[0]); 
		byte  IsOver  = Byte.valueOf(request.get("IsOver")[0]); 
		byte  IsTrip  = Byte.valueOf(request.get("IsTrip")[0]); 
		byte  IsIOk  = Byte.valueOf(request.get("IsIOk")[0]); 
		byte  IsIronAlarm  = Byte.valueOf(request.get("IsIronAlarm")[0]); 
		byte  IsDoorOpen  = Byte.valueOf(request.get("IsDoorOpen")[0]); 
		byte  IsFansOk  = Byte.valueOf(request.get("IsFansOk")[0]); 
		// special handling
//		if(EventType == (byte)0 ||
//				EndTime == 0L) {
//			return null;
//		}
		
		byte[] FaQualifier = { 1, 2, 3, 4, 5,6,7,8,9,10,11,12,13,14,15};
		
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[0]}, 0,
				Bytes.toBytes(TimeFans));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[1]}, 0,
				Bytes.toBytes(TemperatureA));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[2]}, 0,
				Bytes.toBytes(TemperatureB));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[3]}, 0,
				Bytes.toBytes(TemperatureC));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[4]}, 0,
				Bytes.toBytes(TemperatureI));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[5]}, 0,
				new byte[]{IsAOk});
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[6]}, 0,
				new byte[]{IsBOk});
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[7]}, 0,
				new byte[]{IsCOk});
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[8]}, 0,
				new byte[]{IsFansOpen});
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[9]}, 0,
				new byte[]{IsOver});
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[10]}, 0,
				new byte[]{IsTrip});
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[11]}, 0,
				new byte[]{IsIOk});
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[12]}, 0,
				new byte[]{IsIronAlarm});
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[13]}, 0,
				new byte[]{IsDoorOpen});
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[14]}, 0,
				new byte[]{IsFansOk});
//		terminalSettingsBackup(DeviceId, "point2:CompanyId:" + CompanyId + ",DeviceId:" + DeviceId + ",InsertTime:" + DateUtil.formatToHHMMSS(InsertTime));
		List<Put> puts = new ArrayList<Put>();
		puts.add(put);
		return puts;
	}

	@Override
	public boolean put(final List<Put> put) {		
		if(htables.get() == null) {
			try {
				htables.set(HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_temperature_name")));
			} catch (IOException e) {
				e.printStackTrace();
				return false;
			}
		}
		final HTableInterface htable = htables.get();		
		
		try {
			//final long start = System.currentTimeMillis();
			
			htable.put(put);
			htable.flushCommits();// write to hbase immediately
			htable.close();
			//logger.info("put to hbase success..." + (System.currentTimeMillis() - start));
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public ResultScanner getResultScannerWithParameterMap(Scan scan) {
		HTableInterface htable;
		try {
			htable = HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_temperature_name"));
			try {
				long start = System.currentTimeMillis();
				ResultScanner rScaner = htable.getScanner(scan);
				logger.info("get to hbase success..." + (System.currentTimeMillis() - start));
				return rScaner;
			} catch (IOException e) {
				return null;
			}
		} catch (IOException e) {
			return null;
		} 
	}
	
	/**
	 * backup the terminal settings to local file
	 * @param deviceid
	 * @param backupstr
	 */
	private void terminalSettingsBackup(Short deviceid, String backupstr) {
		File directory = new File("");//设定为当前文件夹
		try{
			String folder = directory.getAbsolutePath();
			String path = folder + File.separator + "backup" + File.separator + "temperature" + File.separator 
					+ deviceid + ".txt";
			
			File file = new File(path);
			if(!file.getParentFile().exists()) {
			    file.getParentFile().mkdirs();
			}
			if(!file.exists()) { 
				file.createNewFile();
			}
			if(file.exists()) {
				FileWriter writer = new FileWriter(file, true);
				writer.write(backupstr + "\n");
				writer.flush();
				writer.close();
				
			}
		}catch(Exception e){}
	}

}
