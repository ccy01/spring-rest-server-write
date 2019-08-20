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

import com.nikey.util.DateUtil;
import com.nikey.util.NumberUtil;
import com.nikey.util.PropUtil;

/**
 * @author jayzee
 * @date 28 Sep, 2014
 *	alarmwave HTableMapper
 */
public class AlarmwaveHTableMapper implements HTableMapper {
	
	/**
	 * slfj
	 */
	Logger logger = LoggerFactory.getLogger(getClass());
	
	private ThreadLocal<HTableInterface> htables = new ThreadLocal<HTableInterface>();
	
	public static void main(String[] args) {
		/*String json = PropUtil.getString("JSON");
		Map<String, String[]> jMap = JsonUtil.fromJsonToStringHashMap(json);
		try {
			new AlarmwaveHTableMapper().convertParameterMapToPut(jMap);
		} catch (Exception e) {
			e.printStackTrace();
		}*/
	    for(int i=0; i<48; i++) {
	        System.out.print(String.format("%d.%d", i/2, (i%2)*5) + ",");
	    }
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
		
		logger.info(String.format("consuming %d %s", DeviceId, DateUtil.formatToHHMMSS(InsertTime)));
		
		String EventTypeStr = String.valueOf(request.get("EventType")[0]);
		String[] EventTypeStrArr = EventTypeStr.split(",");
		List<Byte> EventTypeList = new ArrayList<Byte>();
		if(EventTypeStrArr != null) {
			for(String temp : EventTypeStrArr) {
				if(temp != null) {
					if("22011".equals(temp)) {
						EventTypeList.add((byte) 1);
					} else if("22012".equals(temp)) {
						EventTypeList.add((byte) 100);
					}
				}
			}
		}
		byte EventType[] = new byte[EventTypeList.size()];
		for(int in = 0; in<EventTypeList.size(); in++) {
			EventType[in] = EventTypeList.get(in);
		}
		
		long HappenTime = Long.valueOf(request.get("HappenTime")[0]);
		HappenTime = HappenTime * 1000l; // 放大为毫秒值
		float SpaceTime = Float.valueOf(request.get("SpaceTime")[0]);
		
		// bug bix by Jayzee 2015-05-27
		// InsertTime不应该作为rowkey 否则Alarmdata表查询出录波记录后将定位不出Alarmwave表的记录
		Put put = new Put(Bytes.add(Bytes.toBytes(CompanyId),
				Bytes.toBytes(DeviceId),
				Bytes.toBytes(HappenTime)));
		
		String[] UaWaveArr = request.get("UaWave");
		ByteBuffer UaWaveBuf = ByteBuffer.allocate(UaWaveArr.length * 4);
		for(int i=0; i<UaWaveArr.length; i++) {
			UaWaveBuf.put(Bytes.toBytes(Float.valueOf(UaWaveArr[i])));
		}
		String[] UbWaveArr = request.get("UbWave");
		ByteBuffer UbWaveBuf = ByteBuffer.allocate(UbWaveArr.length * 4);
		for(int i=0; i<UbWaveArr.length; i++) {
			UbWaveBuf.put(Bytes.toBytes(Float.valueOf(UbWaveArr[i])));
		}
		String[] UcWaveArr = request.get("UcWave");
		ByteBuffer UcWaveBuf = ByteBuffer.allocate(UcWaveArr.length * 4);
		for(int i=0; i<UcWaveArr.length; i++) {
			UcWaveBuf.put(Bytes.toBytes(Float.valueOf(UcWaveArr[i])));
		}
		String[] IaWaveArr = request.get("IaWave");
		ByteBuffer IaWaveBuf = ByteBuffer.allocate(IaWaveArr.length * 4);
		for(int i=0; i<IaWaveArr.length; i++) {
			IaWaveBuf.put(Bytes.toBytes(Float.valueOf(IaWaveArr[i])));
		}
		String[] IbWaveArr = request.get("IbWave");
		ByteBuffer IbWaveBuf = ByteBuffer.allocate(IbWaveArr.length * 4);
		for(int i=0; i<IbWaveArr.length; i++) {
			IbWaveBuf.put(Bytes.toBytes(Float.valueOf(IbWaveArr[i])));
		}
		String[] IcWaveArr = request.get("IcWave");
		ByteBuffer IcWaveBuf = ByteBuffer.allocate(IcWaveArr.length * 4);
		for(int i=0; i<IcWaveArr.length; i++) {
			IcWaveBuf.put(Bytes.toBytes(Float.valueOf(IcWaveArr[i])));
		}
		
		byte[] FaQualifier = { 1, 2, 3, 4, 5, 6, 7, 8, 9};
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[0]}, 0,
				EventType);
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[1]}, 0,
				Bytes.toBytes(HappenTime));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[2]}, 0,
				Bytes.toBytes(SpaceTime));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[3]}, 0,
				Bytes.toBytes(UaWaveBuf));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[4]}, 0,
				Bytes.toBytes(UbWaveBuf));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[5]}, 0,
				Bytes.toBytes(UcWaveBuf));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[6]}, 0,
				Bytes.toBytes(IaWaveBuf));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[7]}, 0,
				Bytes.toBytes(IbWaveBuf));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[8]}, 0,
				Bytes.toBytes(IcWaveBuf));
//		terminalSettingsBackup(DeviceId, "point2:CompanyId:" + CompanyId + ",DeviceId:" + DeviceId + ",InsertTime:" + DateUtil.formatToHHMMSS(InsertTime));
		List<Put> puts = new ArrayList<Put>();
		puts.add(put);
		return puts;
	}

	@Override
	public boolean put(final List<Put> put) {		
		if(htables.get() == null) {
			try {
				htables.set(HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_alarmwave_name")));
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
			htable = HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_alarmwave_name"));
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
			String path = folder + File.separator + "backup" + File.separator + "alarmwave" + File.separator 
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
