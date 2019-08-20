package com.nikey.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.nikey.hbase.HbaseTablePool;
import com.nikey.hbase.MonitordataHTableMapper;
import com.nikey.util.PropUtil;

public class WriteTest {
	
	public static void main(String[] args) {
		WriteTest test = new WriteTest();
		try {
			test.writeMonitordata();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void writeMonitordata() throws IOException {
		MonitordataHTableMapper mapper = new MonitordataHTableMapper();
		
		short CompanyId = 1;
		short DeviceId = 1001;
		long InsertTime = new Date().getTime();
		byte qualifier = 1;
		float Ua = 220;
		String columnFamily = "U";
		String table = "monitordata";
		
		Put put = new Put(Bytes.add(Bytes.toBytes(CompanyId),
				Bytes.toBytes(DeviceId),
				Bytes.toBytes(InsertTime)));
		put.add(Bytes.toBytes(columnFamily), new byte[]{qualifier}, 0,
				Bytes.toBytes(Ua));
		List<Put> puts = new ArrayList<Put>();
		puts.add(put);
		boolean bool = mapper.put(puts);
		if(bool) {
			System.out.println(table + " write successful......................");
			
			Scan key = new Scan();
			key.setStartRow(Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(DeviceId), Bytes.toBytes(InsertTime - 10)));
			key.setStopRow(Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(DeviceId), Bytes.toBytes(InsertTime + 10)));
			key.addColumn(Bytes.toBytes(columnFamily), new byte[]{qualifier});
			
			ResultScanner scanner = mapper.getResultScannerWithParameterMap(key);
			if(scanner != null) {
				System.out.println(table + " read successful......................");
				
				HTableInterface htable =  HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_monitordata_name"));
				byte[] startKey = Bytes.add(Bytes.toBytes((short)CompanyId), Bytes
						.toBytes((short)DeviceId), Bytes.toBytes(InsertTime));
				Delete delete = new Delete(startKey);
				htable.delete(delete);
				
				System.out.println(table + " delete successful......................");
			} else {
				System.out.println(table + " read failed......................");
			}
		} else {
			System.out.println(table + " write failed......................");
		}
	}

}
