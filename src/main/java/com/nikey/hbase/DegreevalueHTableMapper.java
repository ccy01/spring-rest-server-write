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

import com.nikey.util.DateUtil;
import com.nikey.util.NumberUtil;
import com.nikey.util.PropUtil;

/**
 * @author jayzee
 * @date 28 Sep, 2014
 *	degreevalue HTableMapper
 */
public class DegreevalueHTableMapper implements HTableMapper{
	
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
		
		byte Type = Byte.valueOf(request.get("Type")[0]);
		
		// 当为日电度时减去一秒
		// 底端数据库degreeevaluetable统一存储日电度和小时电度，主键为device_id和insert_time，凌晨的日电度和小时电度重叠
		if(Type == 1) {
			InsertTime = InsertTime - 1000;
		}
		
		Put put = new Put(
					Bytes.add(
							Bytes.add(Bytes.toBytes(CompanyId),
									Bytes.toBytes(DeviceId),
									Bytes.toBytes(InsertTime)
								),
							new byte[]{Type})
				);
		
		StringBuffer sb = new StringBuffer();
		sb.append("point2:CompanyId:" + CompanyId + ",DeviceId:" + DeviceId + ",InsertTime:" + DateUtil.formatToHHMMSS(InsertTime)+"\n");
		
		double PeakEpi = Double.valueOf(request.get("PeakEpi")[0]);
		double FlatEpi = Double.valueOf(request.get("FlatEpi")[0]);
		double ValleyEpi = Double.valueOf(request.get("ValleyEpi")[0]);
		double TotalEpi = Double.valueOf(request.get("TotalEpi")[0]);
		double TotalEpo = Double.valueOf(request.get("TotalEpo")[0]);
		double TotalEQind = Double.valueOf(request.get("TotalEQind")[0]);
		double TotalEQcap = Double.valueOf(request.get("TotalEQcap")[0]);
		float PowerFactor = Float.valueOf(request.get("PowerFactor")[0]);
		
		sb.append("PeakEpi:"+PeakEpi+"\n");
		sb.append("FlatEpi:"+FlatEpi+"\n");
		sb.append("ValleyEpi:"+ValleyEpi+"\n");
		sb.append("TotalEpi:"+TotalEpi+"\n");
		sb.append("TotalEQind:"+TotalEQind+"\n");

		byte[] FaQualifier = { 1, 2, 3, 4, 5, 6, 7, 8};
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[0]}, 0,
				Bytes.toBytes(PeakEpi));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[1]}, 0,
				Bytes.toBytes(FlatEpi));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[2]}, 0,
				Bytes.toBytes(ValleyEpi));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[3]}, 0,
				Bytes.toBytes(TotalEpi));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[4]}, 0,
				Bytes.toBytes(TotalEpo));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[5]}, 0,
				Bytes.toBytes(TotalEQind));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[6]}, 0,
				Bytes.toBytes(TotalEQcap));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[7]}, 0,
				Bytes.toBytes(PowerFactor));
//		terminalSettingsBackup(DeviceId, "point2:CompanyId:" + CompanyId + ",DeviceId:" + DeviceId + ",InsertTime:" + DateUtil.formatToHHMMSS(InsertTime));
//		terminalSettingsBackup2(DeviceId, sb.toString());
		List<Put> puts = new ArrayList<Put>();
		puts.add(put);
		return puts;
	}

	@Override
	public boolean put(final List<Put> put) {		
		if(htables.get() == null) {
			try {
				htables.set(HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_degreevalue_name")));
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
			htable = HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_degreevalue_name"));
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
			String path = folder + File.separator + "backup" + File.separator + "degree" + File.separator 
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

	private void terminalSettingsBackup2(Short deviceid, String backupstr) {
		File directory = new File("");//设定为当前文件夹
		try{
			String folder = directory.getAbsolutePath();
			String path = folder + File.separator + "backup2" + File.separator + "degree" + File.separator 
					+ deviceid + ".txt";
			
			File file = new File(path);
			if(!file.getParentFile().exists()) {
			    file.getParentFile().mkdirs();
			}
			if(!file.exists()) { 
				file.createNewFile();
			}
			if(file.exists()) {
				FileWriter writer = new FileWriter(file);
				writer.write(backupstr + "\n");
				writer.flush();
				writer.close();
				
			}
		}catch(Exception e){}
	}
	
}