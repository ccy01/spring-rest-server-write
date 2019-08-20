package com.nikey.fix;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Date;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.nikey.hbase.HbaseTablePool;
import com.nikey.util.DateUtil;
import com.nikey.util.PropUtil;

/**
 * @author Jayzee
 * @date 2016年7月11日 上午10:28:40
 * 
 * 通过rowkey和Get获取数据
 */
public class GetDataByRowkey {
	
	private static BufferedReader br;
	
	public static void main(String[] args) {
//		System.out.println(DateUtil.parseHHMMSSToDate("2016-06-22 22:44:04").getTime());
//		getFromFile();
//		getFromRowkey((short) 5001, 1466606644000l);
		getFromRowkey((short) 9001, DateUtil.parseHHMMSSToDate("2017-02-17 10:50:00").getTime());
		/*// 5001|2016-06-22 17:00:01|2016-06-22 17:14:17|856
		// 1466586152 --> Wed Jun 22 17:02:32 HKT 2016
		System.out.println(new Date(1466586152000l));*/
	}

	private static void getFromFile() {
		ClassLoader classLoader = GetDataByRowkey.class.getClassLoader();
		File file = new File(classLoader.getResource("tt").getFile());
		
		short CompanyID = 5;
        
        try {
        	HTableInterface htable = HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_monitordata_name"));
        	
    		br = new BufferedReader(new FileReader(file));
    		String frame_msg = br.readLine();
    		
    		while(frame_msg != null) {
    			String details[] = frame_msg.split("\\|");
    			short DeviceID = Short.valueOf(details[1]);
    			long InsertTime = Long.valueOf(details[2]) * 1000l;
    			
    			Get get = new Get(Bytes.add(Bytes.toBytes(CompanyID), Bytes.toBytes(DeviceID), Bytes.toBytes(InsertTime)));
            	Result result = htable.get(get);
            	boolean gocha = false;
            	for(Cell cell : result.rawCells()) {
            		if(cell.getTimestamp() != 4096l) {
                		gocha = true;
                		break;
            		}
            	}
            	
            	if(! gocha) {
            		System.out.println(frame_msg);
            	}
    			
    			frame_msg = br.readLine();
    		}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void getFromRowkey(short DeviceID, long InsertTime) {
		short CompanyID = (short) (DeviceID / 1000);
        
        try {
        	HTableInterface htable = HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_monitordata_name"));
        	
        	Get get = new Get(Bytes.add(Bytes.toBytes(CompanyID), Bytes.toBytes(DeviceID), Bytes.toBytes(InsertTime)));
        	Result result = htable.get(get);
        	for(Cell cell : result.rawCells()) {
        		String colAndQua = Bytes.toString(CellUtil.cloneFamily(cell)) + CellUtil.cloneQualifier(cell)[0];
        		try {
        			System.out.println(colAndQua + ":" + cell.getTimestamp() + ":" + Bytes.toDouble(CellUtil.cloneValue(cell)));
				} catch (Exception e) {
					System.out.println(colAndQua + ":" + cell.getTimestamp() + ":" + Bytes.toFloat(CellUtil.cloneValue(cell)));
				}
        	}
        	
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
