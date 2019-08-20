package com.nikey.test;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.nikey.hbase.HbaseTablePool;
import com.nikey.util.PropUtil;

public class CommerrTest {
	
	public void scanCommerrTest(short CompanyId, short DeviceId, String startTime, String endTime) throws IOException {
		
		HTableInterface htable =  HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_commerr_name"));
		
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
		
	}

}
