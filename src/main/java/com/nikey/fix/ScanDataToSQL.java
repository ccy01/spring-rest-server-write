package com.nikey.fix;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.nikey.hbase.HbaseTablePool;
import com.nikey.util.DateUtil;
import com.nikey.util.ScanUtil;

/**
 * @author Jayzee
 * @date 2016年7月28日 下午6:19:24
 * 构建SQL语句用于填充如下数据表
 * 

CREATE TABLE `data_for_dyf` (
  `DeviceID` int(11) NOT NULL,
  `InsertTime` datetime NOT NULL,
  `Ua` float DEFAULT NULL,
  `Ub` float DEFAULT NULL,
  `Uc` float DEFAULT NULL,
  `Ia` float DEFAULT NULL,
  `Ib` float DEFAULT NULL,
  `Ic` float DEFAULT NULL,
  `Epi` double(255,0) DEFAULT NULL,
  PRIMARY KEY (`DeviceID`,`InsertTime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

 */
public class ScanDataToSQL {
	
	public static void main(String[] args) {
		ScanDataToSQL sd = new ScanDataToSQL();
		
		short CompanyID = 9;
		short DeviceID = 9015;
		String StartTime = "2017-02-09 10:32:36";
		String EndTime = "2017-03-09 10:32:36";
		
		try {
			HTableInterface htable = HbaseTablePool.instance().getHtable("monitordata");
			sd.scanData(CompanyID, DeviceID, StartTime, EndTime, htable);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void scanData(short CompanyID, short DeviceID, String StartTime,
			String EndTime, HTableInterface htable) {
		Scan scan = new Scan();
		try {
			scan.setStartRow(Bytes.add(Bytes.toBytes(CompanyID),
					Bytes.toBytes(DeviceID), Bytes.toBytes(DateUtil.parseHHMMSSToDate(StartTime).getTime())));
			scan.setStopRow(Bytes.add(Bytes.toBytes(CompanyID),
					Bytes.toBytes(DeviceID), Bytes.toBytes(DateUtil.parseHHMMSSToDate(EndTime).getTime() + 1))); // 右开区间
		} catch (Exception e) {
			scan.setStartRow(Bytes.add(Bytes.toBytes(CompanyID),
					Bytes.toBytes(DeviceID), Bytes.toBytes(Long.valueOf(StartTime))));
			scan.setStopRow(Bytes.add(Bytes.toBytes(CompanyID),
					Bytes.toBytes(DeviceID), Bytes.toBytes(Long.valueOf(EndTime) + 1))); // 右开区间
		}
		try {
			scan.setTimeRange(0l, Long.MAX_VALUE);
		} catch (IOException e) {
			e.printStackTrace();
		}
		scan.addColumn(Bytes.toBytes("U"), new byte[] {1});
		scan.addColumn(Bytes.toBytes("U"), new byte[] {2});
		scan.addColumn(Bytes.toBytes("U"), new byte[] {3});
		scan.addColumn(Bytes.toBytes("I"), new byte[] {1});
		scan.addColumn(Bytes.toBytes("I"), new byte[] {2});
		scan.addColumn(Bytes.toBytes("I"), new byte[] {3});
		scan.addColumn(Bytes.toBytes("D"), new byte[] {1});
		String format = "INSERT INTO `test`.`data_for_dyf` (`DeviceID`, `InsertTime`, `Ua`, `Ub`, `Uc`, `Ia`, `Ib`, `Ic`, `Epi`) VALUES ('%d', '%s', '%f', '%f', '%f', '%f', '%f', '%f', '%f');";
		try {
			ResultScanner scanner = htable.getScanner(scan);
			float Ua = 0, Ub = 0, Uc = 0, Ia = 0, Ib = 0, Ic = 0;
			Double Epi = 0.0, cache = null;
			int counter = 0;
			for (Result result : scanner) {
				for (Cell cell : result.rawCells()) {
					if(cell.getTimestamp() != 4096l) {
						String col = Bytes.toString(CellUtil.cloneFamily(cell)) + CellUtil.cloneQualifier(cell)[0];
						switch (col) {
						case "U1":
							Ua = Bytes.toFloat(CellUtil.cloneValue(cell));
							++counter;
							break;
						case "U2":
							Ub = Bytes.toFloat(CellUtil.cloneValue(cell));
							++counter;
							break;
						case "U3":
							Uc = Bytes.toFloat(CellUtil.cloneValue(cell));
							++counter;
							if(counter == 7) {
								String str = String.format(format, ScanUtil.getDeviceIdByCell(cell), DateUtil.formatToHHMMSS(ScanUtil.getTimeByCell(cell)), 
										Ua, Ub, Uc, Ia, Ib, Ic, Epi);
								toFile(str);
							}
							counter = 0;
							break;
						case "I1":
							Ia = Bytes.toFloat(CellUtil.cloneValue(cell));
							++counter;
							break;
						case "I2":
							Ib = Bytes.toFloat(CellUtil.cloneValue(cell));
							++counter;
							break;
						case "I3":
							Ic = Bytes.toFloat(CellUtil.cloneValue(cell));
							++counter;
							break;
						case "D1":
							Epi = Bytes.toDouble(CellUtil.cloneValue(cell));
							if(cache != null && Epi < cache) {
							    System.out.println(DateUtil.formatToHHMMSS(ScanUtil.getTimeByCell(cell)));
							}
							cache = Epi;
							++counter;
							break;
						}
					} 
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void toFile(String sql) {
		File directory = new File("");//设定为当前文件夹
		try{
			String folder = directory.getAbsolutePath();
			String path = folder + File.separator + "data_for_dyf.sql";
			
			File file = new File(path);
			if(!file.getParentFile().exists()) {
			    file.getParentFile().mkdirs();
			}
			if(!file.exists()) { 
				file.createNewFile();
			}
			if(file.exists()) {
				FileWriter writer = new FileWriter(file, true);
				writer.write(sql + "\n");
				writer.flush();
				writer.close();
				
			}
		}catch(Exception e){}
	
	}

}
