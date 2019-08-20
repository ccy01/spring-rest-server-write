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

import com.nikey.util.DataTransferUtil;
import com.nikey.util.PropUtil;
/**
 * 
 * @author ouyang
 * @date 2014-12-04
 * @description virtual group relationtable
 *
 */
public class GrelationHTableMapper implements HTableMapper{
	
	/**
	 * slfj
	 */
	Logger logger = LoggerFactory.getLogger(getClass());
	
	private ThreadLocal<HTableInterface> htables = new ThreadLocal<HTableInterface>();

	@Override
	public List<Put> convertParameterMapToPut(Map<String, String[]> request) {
		// rowkey
		short CompanyId = Short.valueOf(request.get("CompanyId")[0]);
		short DeviceId = Short.valueOf(request.get("DeviceId")[0]);
		
		Put put = new Put(Bytes.add(Bytes.toBytes(CompanyId),
				Bytes.toBytes(DeviceId)));
		
        String[] DeviceIdArrStr = request.get("DeviceIdArr");
        ByteBuffer deviceIdArrBuffer = DataTransferUtil.transferShortStrToByteArr(DeviceIdArrStr);
        float PeakEpiPrice = Float.valueOf(request.get("PeakEpiPrice")[0]);
        float FlatEpiPrice = Float.valueOf(request.get("FlatEpiPrice")[0]);
        float ValleyEpiPrice = Float.valueOf(request.get("ValleyEpiPrice")[0]);
        float Capacity =  Float.valueOf(request.get("Capacity")[0]);
        float CapacityPrice =  Float.valueOf(request.get("CapacityPrice")[0]);
        float MaxDemandPrice = Float.valueOf(request.get("MaxDemandPrice")[0]);
        byte  CapacityOrDemand = Byte.valueOf(request.get("CapacityOrDemand")[0]);
        String[] PFTArrStr =  request.get("PFTArr");
        ByteBuffer   pftArrBuffer = DataTransferUtil.transferFloatStrToByteArr(PFTArrStr);
        String[] FreeRateStr =  request.get("FreeRate");
        ByteBuffer   freeRateBuffer = DataTransferUtil.transferFloatStrToByteArr(FreeRateStr);
        
        
		// special handling
		
		byte[] FaQualifier = { 1, 2, 3, 4, 5,6,7,8,9,10};
		
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[0]}, 0,
				Bytes.toBytes(deviceIdArrBuffer));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[1]}, 0,
				Bytes.toBytes(PeakEpiPrice));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[2]}, 0,
				Bytes.toBytes(FlatEpiPrice));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[3]}, 0,
				Bytes.toBytes(ValleyEpiPrice));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[4]}, 0,
				Bytes.toBytes(Capacity));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[5]}, 0,
				Bytes.toBytes(CapacityPrice));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[6]}, 0,
				Bytes.toBytes(MaxDemandPrice));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[7]}, 0,
				new byte[]{CapacityOrDemand});
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[8]}, 0,
				Bytes.toBytes(pftArrBuffer));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[9]}, 0,
				Bytes.toBytes(freeRateBuffer));
//		terminalSettingsBackup(DeviceId, "point2:CompanyId:" + CompanyId + ",DeviceId:" + DeviceId );
		List<Put> puts = new ArrayList<Put>();
		puts.add(put);
		return puts;
	}

	@Override
	public boolean put(final List<Put> put) {		
		if(htables.get() == null) {
			try {
				htables.set(HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_vrelation_name")));
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
		}
	}

	@Override
	public ResultScanner getResultScannerWithParameterMap(Scan scan) {
		HTableInterface htable;
		try {
			htable = HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_grelation_name"));
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
			String path = folder + File.separator + "backup" + File.separator + "grelation" + File.separator 
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

