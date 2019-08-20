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

public class DemandvalueHTableMapper implements HTableMapper {

	/**
	 * slfj
	 */
	Logger logger = LoggerFactory.getLogger(getClass());
	
	private ThreadLocal<HTableInterface> htables = new ThreadLocal<HTableInterface>();
	
	@Override
	public List<Put> convertParameterMapToPut(Map<String, String[]> request) {
		return null;
		/*short CompanyId = 0;
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
		
		float MFPdemand = Float.valueOf(request.get("MFPdemand")[0]);
		long MFPdemandT = Long.valueOf(request.get("MFPdemandT")[0]);
		MFPdemandT = MFPdemandT * 1000l; // 放大为毫秒值
		float MBPdemand = Float.valueOf(request.get("MBPdemand")[0]);
		long MBPdemandT = Long.valueOf(request.get("MBPdemandT")[0]);
		MBPdemandT = MBPdemandT * 1000l; // 放大为毫秒值
		float MFQdemand = Float.valueOf(request.get("MFQdemand")[0]);
		long MFQdemandT = Long.valueOf(request.get("MFQdemandT")[0]);
		MFQdemandT = MFQdemandT * 1000l; // 放大为毫秒值
		float MBQdemand = Float.valueOf(request.get("MBQdemand")[0]);
		long MBQdemandT = Long.valueOf(request.get("MBQdemandT")[0]);
		MBQdemandT = MBQdemandT * 1000l; // 放大为毫秒值
		
		byte[] FaQualifier = { 1, 2, 3, 4, 5, 6, 7, 8 };
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[0]}, 0,
				Bytes.toBytes(MFPdemand));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[1]}, 0,
				Bytes.toBytes(MFPdemandT));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[2]}, 0,
				Bytes.toBytes(MBPdemand));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[3]}, 0,
				Bytes.toBytes(MBPdemandT));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[4]}, 0,
				Bytes.toBytes(MFQdemand));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[5]}, 0,
				Bytes.toBytes(MFQdemandT));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[6]}, 0,
				Bytes.toBytes(MBQdemand));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[7]}, 0,
				Bytes.toBytes(MBQdemandT));
//		terminalSettingsBackup(DeviceId, "point2:CompanyId:" + CompanyId + ",DeviceId:" + DeviceId + ",InsertTime:" + DateUtil.formatToHHMMSS(InsertTime));
		List<Put> puts = new ArrayList<Put>();
		puts.add(put);
		return puts;*/
	}

	@Override
	public boolean put(final List<Put> put) {		
		/*if(htables.get() == null) {
			try {
				htables.set(HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_demandvalue_name")));
			} catch (IOException e) {
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
		} catch (Exception ingnore) {
			return false;
		}*/
		// TODO 默认不存储最大需量
		return true;
	}

	@Override
	public ResultScanner getResultScannerWithParameterMap(Scan scan) {
		HTableInterface htable;
		try {
			htable = HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_demandvalue_name"));
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
			String path = folder + File.separator + "backup" + File.separator + "demandvalue" + File.separator 
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
