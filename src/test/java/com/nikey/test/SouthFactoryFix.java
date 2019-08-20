package com.nikey.test;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nikey.hbase.HbaseTablePool;
import com.nikey.util.ScanUtil;

public class SouthFactoryFix {
	
	Logger logger = LoggerFactory.getLogger(getClass());
	
	SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	/*java -jar delete_south_factory2.jar 2014-12-01_00:00:00 2015-01-01_00:00:00 U monitordata true;
	java -jar delete_south_factory2.jar 2015-01-01_00:00:00 2015-02-01_00:00:00 U monitordata true;
	java -jar delete_south_factory2.jar 2015-02-01_00:00:00 2015-03-01_00:00:00 U monitordata true;
	java -jar delete_south_factory2.jar 2015-03-01_00:00:00 2015-04-01_00:00:00 U monitordata true;
	java -jar delete_south_factory2.jar 2015-04-01_00:00:00 2015-05-01_00:00:00 U monitordata true;*/
	
	public static void main(String[] args) {
		SouthFactoryFix deleteSouthFactoryTest = new SouthFactoryFix();
		try {
			
			if(args == null || args.length != 5) {
				args = new String[5];
				args[0] = "2014-12-04 00:00:00"; // start time
				args[1] = "2014-12-15 18:00:00"; // end time
				args[2] = "A"; // column family
				args[3] = "group"; // table name
				args[4] = "false"; // need to delete and verify
			} else {
				// 2014-12-04_00:00:00
				args[0] = args[0].replace('_', ' ');
				args[1] = args[1].replace('_', ' ');
			}
			
			short CompanyId = 3; 
			short StartDeviceId = 3000;
			short EndDeviceId = 3014;
			String startTime = args[0];
			String endTime = args[1];
			String columnFamily = args[2];
			String tableName = args[3];
			boolean deleted = Boolean.valueOf(args[4]);
			
			HTableInterface htable =  HbaseTablePool.instance().getHtable(tableName);
			// scan
			List<Delete> deletes = deleteSouthFactoryTest.scan(CompanyId, StartDeviceId, EndDeviceId, startTime, endTime, htable, columnFamily);
			// delete and verify whether delete successfully
			if(deleted) {
				deleteSouthFactoryTest.delete(deletes, htable);
				deleteSouthFactoryTest.scan(CompanyId, StartDeviceId, EndDeviceId, startTime, endTime, htable, columnFamily);
			}
			deleteSouthFactoryTest.logger.info("happy ending...");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	// scan rowkeys
	public List<Delete> scan(short CompanyId, short StartDeviceId, short EndDeviceId, String startTime, String endTime, HTableInterface htable, String columnFamily) {
		Scan scan = null;
		long compareTime = 0;
		try {
			compareTime = format.parse(endTime).getTime();
			byte[] startKey = Bytes.add(Bytes.toBytes((short)CompanyId), Bytes
					.toBytes((short)StartDeviceId), Bytes.toBytes(format.parse(startTime).getTime()));
			byte[] endKey = Bytes.add(Bytes.toBytes((short)CompanyId), Bytes
					.toBytes((short)EndDeviceId), Bytes.toBytes(format.parse(endTime).getTime()));
			 scan = new Scan(startKey, endKey);
		} catch (Exception e) {
			compareTime = Long.valueOf(endTime);
			byte[] startKey = Bytes.add(Bytes.toBytes((short)CompanyId), Bytes
					.toBytes((short)StartDeviceId), Bytes.toBytes(Long.valueOf(startTime)));
			byte[] endKey = Bytes.add(Bytes.toBytes((short)CompanyId), Bytes
					.toBytes((short)EndDeviceId), Bytes.toBytes(Long.valueOf(endTime)));
			scan = new Scan(startKey, endKey);
		}
		
		scan.addColumn(Bytes.toBytes(columnFamily), new byte[]{Byte.valueOf("1")});
		
		scan.setCaching(100);
		scan.setBatch(2);
		
		List<Delete> deletes = new ArrayList<Delete>();
		
		try {
			ResultScanner rScaner = htable.getScanner(scan);
			long counter = 0;
			for (Result r : rScaner) {
				long InsertTime = 0;
				short DeviceID = 0;
				for (Cell cell : r.rawCells()) {
					InsertTime = ScanUtil.getTimeByCell(cell);
					DeviceID = ScanUtil.getDeviceIdByCell(cell);
					break;
				}
				if(InsertTime <= compareTime) {
					counter++;
					logger.info("rowkey : " + CompanyId + ", " + DeviceID + ", " + format.format(new Date(InsertTime)) + ", " + counter);
					// form deletes
					Delete delete = new Delete(Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(DeviceID), Bytes.toBytes(InsertTime)));
					deletes.add(delete);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return deletes;
	}
	
	// delete batch deletes
	public void delete(List<Delete> deletes, HTableInterface htable) {
		Object [] results = new Object[deletes.size()];
		try {
			htable.batch(deletes, results);
			logger.info("delete done...");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
