package com.nikey.test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.nikey.hbase.HbaseTablePool;
import com.nikey.util.DateUtil;
import com.nikey.util.ScanUtil;

public class KeyTest {
	
	public static void main(String[] args) throws ParseException, IOException {
		KeyTest key = new KeyTest();
		short companyId = Short.valueOf(args[0]);
		short deviceId = Short.valueOf(args[1]);
		key.scanKey(companyId, deviceId, args[2], args[3], args[4]);
	}
	
	public void scanKey(short CompanyId, short DeviceId, String startTime, String endTime, String table) throws ParseException, IOException {
		HTableInterface htable =  HbaseTablePool.instance().getHtable(table);
				
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		Scan scan = null;
		try {
			byte[] startKey = Bytes.add(Bytes.toBytes((short)CompanyId), Bytes
					.toBytes((short)DeviceId), Bytes.toBytes(format.parse(startTime).getTime()));
			byte[] endKey = Bytes.add(Bytes.toBytes((short)CompanyId), Bytes
					.toBytes((short)DeviceId), Bytes.toBytes(format.parse(endTime).getTime()));
			 scan = new Scan(startKey, endKey);
		} catch (Exception e) {
			byte[] startKey = Bytes.add(Bytes.toBytes((short)CompanyId), Bytes
					.toBytes((short)DeviceId), Bytes.toBytes(Long.valueOf(startTime)));
			byte[] endKey = Bytes.add(Bytes.toBytes((short)CompanyId), Bytes
					.toBytes((short)DeviceId), Bytes.toBytes(Long.valueOf(endTime)));
			scan = new Scan(startKey, endKey);
		}
		
		try {
			ResultScanner rScaner = htable.getScanner(scan);
			StringBuffer sb = new StringBuffer();
			for (Result r : rScaner) {
				for (Cell c : r.rawCells()) {
					sb.append(
							ScanUtil.getDeviceIdByCell(c) +
					":" + DateUtil.formatToHHMMSS(new Date(ScanUtil.getTimeByCell(c)))
							);
					writeToFile(sb.toString());
					sb = new StringBuffer();
					break;
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void writeToFile(String msg) {
		File directory = new File("");//设定为当前文件夹
		try{
			String folder = directory.getAbsolutePath();
			String path = folder + File.separator + "data"+ ".txt";
			
			File file = new File(path);
			if(!file.getParentFile().exists()) {
			    file.getParentFile().mkdirs();
			}
			if(!file.exists()) { 
				file.createNewFile();
			}
			if(file.exists()) {
				FileWriter writer = new FileWriter(file, true);
				writer.write(msg + "\n"); // 换行
				writer.flush();
				writer.close();
				
			}
		}catch(Exception e){}
	}

}
