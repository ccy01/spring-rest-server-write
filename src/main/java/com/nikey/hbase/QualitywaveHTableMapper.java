package com.nikey.hbase;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
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
 * @author jayzee
 * @date 28 Sep, 2014
 *	Qualitywave HTable Mapper instance
 */
public class QualitywaveHTableMapper implements HTableMapper{
	
	/**
	 * slfj
	 */
	Logger logger = LoggerFactory.getLogger(getClass());
	
	private ThreadLocal<HTableInterface> htables = new ThreadLocal<HTableInterface>();
	
	public static void main(String[] args) {
		String json = PropUtil.getString("json2");
		System.out.println(json.split(",").length);
//		Map<String, String[]> jMap = JsonUtil.fromJsonToStringHashMap(json);
//		try {
//			new QualitywaveHTableMapper().convertParameterMapToPut(jMap);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
	}

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
		
		float PersistTime = Float.valueOf(request.get("PersistTime")[0]);
		long HappenTime = Long.valueOf(request.get("HappenTime")[0]);
		HappenTime = HappenTime * 1000l; // 放大为毫秒值
		float Eua = Float.valueOf(request.get("Eua")[0]);
		float Eub = Float.valueOf(request.get("Eub")[0]);
		float Euc = Float.valueOf(request.get("Euc")[0]);
		// WARN 约定质量录波的数据类型表示
		byte CauseId = 0;
		if("21007".equals(request.get("CauseId")[0])) {
			CauseId = 1;
		} else if("21006".equals(request.get("CauseId")[0])) {
			CauseId = 2;
		}
		
		// bug bix by Jayzee 2015-05-27
		// InsertTime不应该作为rowkey 否则Alarmdata表查询出录波记录后将定位不出Qualitywave表的记录
		Put put = new Put(Bytes.add(Bytes.toBytes(CompanyId),
				Bytes.toBytes(DeviceId),
				Bytes.toBytes(HappenTime)));
		
		String[] QwaveArr = request.get("Qwave");
		/*int RealCount = Integer.valueOf(request.get("Count")[0]) / 6;
		if(RealCount == 0) throw new RuntimeException("Qualitywave count can't be null");*/
		int RealCount = 320; // 目前版本为 40*8 = 320 个点
		
		int Count = QwaveArr.length / 6;
		if(Count == 0) throw new RuntimeException("Qualitywave count can't be null");
		
		ByteBuffer buf1 = ByteBuffer.allocate(RealCount * 4);
		ByteBuffer buf2 = ByteBuffer.allocate(RealCount * 4);
		ByteBuffer buf3 = ByteBuffer.allocate(RealCount * 4);
		ByteBuffer buf4 = ByteBuffer.allocate(RealCount * 4);
		ByteBuffer buf5 = ByteBuffer.allocate(RealCount * 4);
		ByteBuffer buf6 = ByteBuffer.allocate(RealCount * 4);

		for(int i=Count*0; i<Count*0+RealCount; i++) {
			buf1.put(Bytes.toBytes(Float.valueOf(QwaveArr[i])));
		}
		for(int i=Count*1; i<Count*1+RealCount; i++) {
			buf2.put(Bytes.toBytes(Float.valueOf(QwaveArr[i])));
		}
		for(int i=Count*2; i<Count*2+RealCount; i++) {
			buf3.put(Bytes.toBytes(Float.valueOf(QwaveArr[i])));
		}
		for(int i=Count*3; i<Count*3+RealCount; i++) {
			buf4.put(Bytes.toBytes(Float.valueOf(QwaveArr[i])));
		}
		for(int i=Count*4; i<Count*4+RealCount; i++) {
			buf5.put(Bytes.toBytes(Float.valueOf(QwaveArr[i])));
		}
		for(int i=Count*5; i<Count*5+RealCount; i++) {
			buf6.put(Bytes.toBytes(Float.valueOf(QwaveArr[i])));
		}
		
		byte[] FaQualifier = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13};
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[0]}, 0,
				Bytes.toBytes(PersistTime));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[1]}, 0,
				Bytes.toBytes(HappenTime));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[2]}, 0,
				Bytes.toBytes(Eua));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[3]}, 0,
				Bytes.toBytes(Eub));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[4]}, 0,
				Bytes.toBytes(Euc));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[5]}, 0,
				new byte[]{CauseId});
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[6]}, 0,
				Bytes.toBytes(buf1));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[7]}, 0,
				Bytes.toBytes(buf2));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[8]}, 0,
				Bytes.toBytes(buf3));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[9]}, 0,
				Bytes.toBytes(buf4));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[10]}, 0,
				Bytes.toBytes(buf5));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[11]}, 0,
				Bytes.toBytes(buf6));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[12]}, 0,
				new byte[]{8}); // hard code
//		terminalSettingsBackup(DeviceId, "point2:CompanyId:" + CompanyId + ",DeviceId:" + DeviceId + ",HappenTime:" + DateUtil.formatToHHMMSS(HappenTime));
		List<Put> puts = new ArrayList<Put>();
		puts.add(put);
		return puts;
	}

	@Override
	public boolean put(final List<Put> put) {		
		if(htables.get() == null) {
			try {
				htables.set(HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_qualitywave_name")));
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
			htable = HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_qualitywave_name"));
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
			String path = folder + File.separator + "backup" + File.separator + "quality" + File.separator 
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
