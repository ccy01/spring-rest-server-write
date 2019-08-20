package com.nikey.test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.nikey.hbase.AlarmdataHTableMapper;
import com.nikey.hbase.AlarmwaveHTableMapper;
import com.nikey.hbase.HbaseTablePool;
import com.nikey.hbase.QualitywaveHTableMapper;
import com.nikey.util.PropUtil;
import com.nikey.util.ScanUtil;

public class AlarmdataFix {
	
	public static void main(String[] args) throws IOException {
		AlarmdataHTableMapper mapper = new AlarmdataHTableMapper();
		QualitywaveHTableMapper mapperQ = new QualitywaveHTableMapper();
		AlarmwaveHTableMapper mapperA = new AlarmwaveHTableMapper();
		AlarmdataFix fix = new AlarmdataFix();
		// transformOldData -> deleteOld -> deleteNew -> testNewData
		// deleteQualityAndWave -> scanQualityToAlarm -> scanWaveToAlarm
//		fix.transformOldData(mapper);
		fix.deleteOld(mapper);
		fix.deleteNew(mapper);
//		fix.testNewData(mapper);
		fix.deleteQualityAndWave(mapper);
		fix.scanQualityToAlarm(mapper,mapperQ);
		fix.scanWaveToAlarm(mapper, mapperA);
		System.out.println("happing ending...");
		System.exit(0);
	}
	
	private void scanWaveToAlarm(AlarmdataHTableMapper mapper,AlarmwaveHTableMapper mapperA) throws IOException {
		short CompanyId = 1;
		String startTime = "2015-01-01 00:00:00";
		String endTime = "2015-05-01 00:00:00";
		
		HTableInterface htable =  HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_alarmwave_name"));
				
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		Scan scan = null;
		try {
			byte[] startKey = Bytes.add(Bytes.toBytes((short)CompanyId), Bytes
					.toBytes((short)1000), Bytes.toBytes(format.parse(startTime).getTime()));
			byte[] endKey = Bytes.add(Bytes.toBytes((short)CompanyId), Bytes
					.toBytes((short)1025), Bytes.toBytes(format.parse(endTime).getTime()));
			 scan = new Scan(startKey, endKey);
		} catch (Exception e) {
			byte[] startKey = Bytes.add(Bytes.toBytes((short)CompanyId), Bytes
					.toBytes((short)1000), Bytes.toBytes(Long.valueOf(startTime)));
			byte[] endKey = Bytes.add(Bytes.toBytes((short)CompanyId), Bytes
					.toBytes((short)1025), Bytes.toBytes(Long.valueOf(endTime)));
			scan = new Scan(startKey, endKey);
		}
		
		scan.setCaching(100);
		scan.setBatch(2);
		
		try {
			ResultScanner rScaner = htable.getScanner(scan);
			List<Put> puts = new ArrayList<Put>();
			for (Result r : rScaner) {
				for (Cell c : r.rawCells()) {
					int qua = CellUtil.cloneQualifier(c)[0];
					if(qua == 1){
						byte [] event = CellUtil.cloneValue(c);
						if(event.length > 1) {
							System.out.println(event);
						} 
						puts.add(getPutByRowkey(ScanUtil.getDeviceIdByCell(c), ScanUtil.getTimeByCell(c), CellUtil.cloneValue(c)[0]));
					}
				}
			}
			mapper.put(puts);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void scanQualityToAlarm(AlarmdataHTableMapper mapper,QualitywaveHTableMapper mapperQ) throws IOException {
		short CompanyId = 1;
		String startTime = "2015-01-01 00:00:00";
		String endTime = "2015-05-01 00:00:00";
		
		HTableInterface htable =  HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_qualitywave_name"));
				
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		Scan scan = null;
		try {
			byte[] startKey = Bytes.add(Bytes.toBytes((short)CompanyId), Bytes
					.toBytes((short)1000), Bytes.toBytes(format.parse(startTime).getTime()));
			byte[] endKey = Bytes.add(Bytes.toBytes((short)CompanyId), Bytes
					.toBytes((short)1025), Bytes.toBytes(format.parse(endTime).getTime()));
			 scan = new Scan(startKey, endKey);
		} catch (Exception e) {
			byte[] startKey = Bytes.add(Bytes.toBytes((short)CompanyId), Bytes
					.toBytes((short)1000), Bytes.toBytes(Long.valueOf(startTime)));
			byte[] endKey = Bytes.add(Bytes.toBytes((short)CompanyId), Bytes
					.toBytes((short)1025), Bytes.toBytes(Long.valueOf(endTime)));
			scan = new Scan(startKey, endKey);
		}
		
		scan.setCaching(100);
		scan.setBatch(2);
		
		try {
			ResultScanner rScaner = htable.getScanner(scan);
			List<Put> puts = new ArrayList<Put>();
			for (Result r : rScaner) {
				for (Cell c : r.rawCells()) {
					int qua = CellUtil.cloneQualifier(c)[0];
					if(qua == 6){
						puts.add(getPutByRowkey(ScanUtil.getDeviceIdByCell(c), ScanUtil.getTimeByCell(c), CellUtil.cloneValue(c)[0]));
					}
				}
			}
			mapper.put(puts);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public Put getPutByRowkey(short DeviceId, long InsertTime, short EventType) {
		Put put = new Put(
				Bytes.add(
					Bytes.add(
						Bytes.toBytes((short) 1),
						Bytes.toBytes(DeviceId),
						Bytes.toBytes(InsertTime)), 
					Bytes.toBytes(EventType)
				)
			);
		byte[] FaQualifier = { 1, 2, 3, 4 };
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[0]}, 0,
				Bytes.toBytes(InsertTime));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[1]}, 0,
				Bytes.toBytes((long)0));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[2]}, 0,
				Bytes.toBytes((float)0));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[3]}, 0,
				new byte[]{(byte)0});
		return put;
	}


	public void deleteQualityAndWave(AlarmdataHTableMapper mapper) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			// row key value
			short CompanyId = 1;
			short startDevice = 0;
			short endDevice = 1025;
			long startTime = format.parse("2015-01-01 00:00:00").getTime();
			long endTime = format.parse("2015-05-01 00:00:00").getTime();
			short startEventType = 1;
			short endEventType = 128;
			
			// start key & end key
			Scan scan1 = new Scan();
			scan1.setStartRow(Bytes.add(Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(startDevice), Bytes.toBytes(startTime)), Bytes.toBytes(startEventType)));
			scan1.setStopRow(Bytes.add(Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(endDevice), Bytes.toBytes(endTime)), Bytes.toBytes(endEventType)));
			
			// delete list
			List<Delete> dels = new ArrayList<Delete>();
			
			ResultScanner scanner = mapper.getResultScannerWithParameterMap(scan1);
			long count = 0;
			for (Result result : scanner) {
				count++;
				long EndTime = 0;
				long Duration = 0;
				float Currentvalue = 0;
				byte Flag = 0;
				short EventType = 0;
				short DeviceId = 0;
				long InsertTime = 0;
				boolean isOld = false;
				for (Cell cell : result.rawCells()) { 
					// 匹配qualifier:考虑值为空的情况
					byte qulifier = CellUtil.cloneQualifier(cell)[0];
					// if you want to get <CompanyId> or <DeviceId>, then use ScanUtil.getDeviceIdByCell(cell) or ScanUtil.getCompanyIdByCell(cell)
					InsertTime = ScanUtil.getTimeByCell(cell);
					DeviceId = ScanUtil.getDeviceIdByCell(cell);
					EventType = ScanUtil.getEventTypeByCell(cell);
					
					if(qulifier == 1) {
						EndTime = Bytes.toLong(CellUtil.cloneValue(cell));
					} else if(qulifier == 2) {
						Duration = Bytes.toLong(CellUtil.cloneValue(cell));
					} else if(qulifier == 3) {
						Currentvalue = Bytes.toFloat(CellUtil.cloneValue(cell));
					} else if(qulifier == 4) {
						Flag = CellUtil.cloneValue(cell)[0];
					} else if(qulifier == 5) {
						EventType = CellUtil.cloneValue(cell)[0];
						isOld = true;
					}
				}
				System.out.println(EndTime);
				System.out.println(Duration);
				System.out.println(Currentvalue);
				System.out.println(Flag);
				System.out.println(EventType);
				System.out.println(DeviceId);
				System.out.println(InsertTime);
				System.out.println();
				if(isOld || EventType == 0 || EventType == 67 || EventType == 68 || (EventType >= 43 && EventType <= 51)) {
					dels.add(new Delete(Bytes.add(Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(DeviceId), Bytes.toBytes(InsertTime)), Bytes.toBytes(EventType))));
				}
			}
			HTableInterface htable =  HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_alarmdata_name"));
			for(Delete del : dels) {
				htable.delete(del);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void deleteNew(AlarmdataHTableMapper mapper) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			// row key value
			short CompanyId = 1;
			short startDevice = 0;
			short endDevice = 1025;
			long startTime = format.parse("2015-03-01 00:00:00").getTime();
			long endTime = format.parse("2015-05-01 00:00:00").getTime();
			short startEventType = 1;
			short endEventType = 128;
			
			// start key & end key
			Scan scan1 = new Scan();
			scan1.setStartRow(Bytes.add(Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(startDevice), Bytes.toBytes(startTime)), Bytes.toBytes(startEventType)));
			scan1.setStopRow(Bytes.add(Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(endDevice), Bytes.toBytes(endTime)), Bytes.toBytes(endEventType)));
			
			// delete list
			List<Delete> dels = new ArrayList<Delete>();
			
			ResultScanner scanner = mapper.getResultScannerWithParameterMap(scan1);
			long count = 0;
			for (Result result : scanner) {
				count++;
				long EndTime = 0;
				long Duration = 0;
				float Currentvalue = 0;
				byte Flag = 0;
				short EventType = 0;
				short DeviceId = 0;
				long InsertTime = 0;
				boolean isOld = false;
				for (Cell cell : result.rawCells()) { 
					// 匹配qualifier:考虑值为空的情况
					byte qulifier = CellUtil.cloneQualifier(cell)[0];
					// if you want to get <CompanyId> or <DeviceId>, then use ScanUtil.getDeviceIdByCell(cell) or ScanUtil.getCompanyIdByCell(cell)
					InsertTime = ScanUtil.getTimeByCell(cell);
					DeviceId = ScanUtil.getDeviceIdByCell(cell);
					EventType = ScanUtil.getEventTypeByCell(cell);
					
					if(qulifier == 1) {
						EndTime = Bytes.toLong(CellUtil.cloneValue(cell));
					} else if(qulifier == 2) {
						Duration = Bytes.toLong(CellUtil.cloneValue(cell));
					} else if(qulifier == 3) {
						Currentvalue = Bytes.toFloat(CellUtil.cloneValue(cell));
					} else if(qulifier == 4) {
						Flag = CellUtil.cloneValue(cell)[0];
					} else if(qulifier == 5) {
						EventType = CellUtil.cloneValue(cell)[0];
						isOld = true;
					}
				}
				System.out.println(EndTime);
				System.out.println(Duration);
				System.out.println(Currentvalue);
				System.out.println(Flag);
				System.out.println(EventType);
				System.out.println(DeviceId);
				System.out.println(InsertTime);
				System.out.println();
				if(isOld || EventType == 0) {
					dels.add(new Delete(Bytes.add(Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(DeviceId), Bytes.toBytes(InsertTime)), Bytes.toBytes(EventType))));
				}
			}
			HTableInterface htable =  HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_alarmdata_name"));
			for(Delete del : dels) {
				htable.delete(del);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void testNewData(AlarmdataHTableMapper mapper) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			// row key value
			short CompanyId = 1;
			short startDevice = 0;
			short endDevice = 1025;
			long startTime = format.parse("2015-03-01 00:00:00").getTime();
			long endTime = format.parse("2015-05-01 00:00:00").getTime();
			short startEventType = 1;
			short endEventType = 128;
			
			// start key & end key
			Scan scan1 = new Scan();
			scan1.setStartRow(Bytes.add(Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(startDevice), Bytes.toBytes(startTime)), Bytes.toBytes(startEventType)));
			scan1.setStopRow(Bytes.add(Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(endDevice), Bytes.toBytes(endTime)), Bytes.toBytes(endEventType)));
			
			ResultScanner scanner = mapper.getResultScannerWithParameterMap(scan1);
			for (Result result : scanner) {
				long EndTime = 0;
				long Duration = 0;
				float Currentvalue = 0;
				byte Flag = 0;
				short EventType = 0;
				short DeviceId = 0;
				long InsertTime = 0;
				for (Cell cell : result.rawCells()) { 
					// 匹配qualifier:考虑值为空的情况
					byte qulifier = CellUtil.cloneQualifier(cell)[0];
					// if you want to get <CompanyId> or <DeviceId>, then use ScanUtil.getDeviceIdByCell(cell) or ScanUtil.getCompanyIdByCell(cell)
					InsertTime = ScanUtil.getTimeByCell(cell);
					DeviceId = ScanUtil.getDeviceIdByCell(cell);
					EventType = ScanUtil.getEventTypeByCell(cell);
					
					if(qulifier == 1) {
						EndTime = Bytes.toLong(CellUtil.cloneValue(cell));
					} else if(qulifier == 2) {
						Duration = Bytes.toLong(CellUtil.cloneValue(cell));
					} else if(qulifier == 3) {
						Currentvalue = Bytes.toFloat(CellUtil.cloneValue(cell));
					} else if(qulifier == 4) {
						Flag = CellUtil.cloneValue(cell)[0];
					} else if(qulifier == 5) {
						EventType = CellUtil.cloneValue(cell)[0];
					}
				}
				System.out.println(EndTime);
				System.out.println(Duration);
				System.out.println(Currentvalue);
				System.out.println(Flag);
				System.out.println(EventType);
				System.out.println(DeviceId);
				System.out.println(InsertTime);
				System.out.println();
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void transformOldData(AlarmdataHTableMapper mapper) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			// row key value
			short CompanyId = 1;
			short startDevice = 0;
			short endDevice = 1025;
			long startTime = format.parse("2015-03-01 00:00:00").getTime();
			long endTime = format.parse("2015-05-01 00:00:00").getTime();
			
			// start key & end key
			Scan scan1 = new Scan();
			scan1.setStartRow(Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(startDevice), Bytes.toBytes(startTime)));
			scan1.setStopRow(Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(endDevice), Bytes.toBytes(endTime)));
			
			// put list
			byte[] FaQualifier = { 1, 2, 3, 4 };
			List<Put> puts = new ArrayList<Put>();
			
			ResultScanner scanner = mapper.getResultScannerWithParameterMap(scan1);
			for (Result result : scanner) {
				long EndTime = 0;
				long Duration = 0;
				float Currentvalue = 0;
				byte Flag = 0;
				short EventType = 0;
				short DeviceId = 0;
				long InsertTime = 0;
				for (Cell cell : result.rawCells()) { 
					// 匹配qualifier:考虑值为空的情况
					byte qulifier = CellUtil.cloneQualifier(cell)[0];
					// if you want to get <CompanyId> or <DeviceId>, then use ScanUtil.getDeviceIdByCell(cell) or ScanUtil.getCompanyIdByCell(cell)
					InsertTime = ScanUtil.getTimeByCell(cell);
					DeviceId = ScanUtil.getDeviceIdByCell(cell);
					
					if(qulifier == 1) {
						EndTime = Bytes.toLong(CellUtil.cloneValue(cell));
					} else if(qulifier == 2) {
						Duration = Bytes.toLong(CellUtil.cloneValue(cell));
						// 以前错误放大了1000
						Duration /= 1000;
					} else if(qulifier == 3) {
						Currentvalue = Bytes.toFloat(CellUtil.cloneValue(cell));
					} else if(qulifier == 4) {
						Flag = CellUtil.cloneValue(cell)[0];
					} else if(qulifier == 5) {
						EventType = CellUtil.cloneValue(cell)[0];;
					} 
				}
				Put put = new Put(
						Bytes.add(
							Bytes.add(
								Bytes.toBytes(CompanyId),
								Bytes.toBytes(DeviceId),
								Bytes.toBytes(InsertTime)), 
							Bytes.toBytes(EventType)
						)
					);
				put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[0]}, 0,
						Bytes.toBytes(EndTime));
				put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[1]}, 0,
						Bytes.toBytes(Duration));
				put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[2]}, 0,
						Bytes.toBytes(Currentvalue));
				put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[3]}, 0,
						new byte[]{Flag});
				puts.add(put);
			}
			
			mapper.put(puts);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void deleteOld(AlarmdataHTableMapper mapper) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			// row key value
			short CompanyId = 1;
			short startDevice = 0;
			short endDevice = 1025;
			long startTime = format.parse("2015-03-01 00:00:00").getTime();
			long endTime = format.parse("2015-05-01 00:00:00").getTime();
			
			// start key & end key
			Scan scan1 = new Scan();
			scan1.setStartRow(Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(startDevice), Bytes.toBytes(startTime)));
			scan1.setStopRow(Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(endDevice), Bytes.toBytes(endTime)));
			
			// delete list
			List<Delete> dels = new ArrayList<Delete>();
			
			ResultScanner scanner = mapper.getResultScannerWithParameterMap(scan1);
			long count = 0;
			for (Result result : scanner) {
				count++;
				long EndTime = 0;
				long Duration = 0;
				float Currentvalue = 0;
				byte Flag = 0;
				short EventType = 0;
				short DeviceId = 0;
				long InsertTime = 0;
				boolean isOld = false;
				for (Cell cell : result.rawCells()) { 
					// 匹配qualifier:考虑值为空的情况
					byte qulifier = CellUtil.cloneQualifier(cell)[0];
					// if you want to get <CompanyId> or <DeviceId>, then use ScanUtil.getDeviceIdByCell(cell) or ScanUtil.getCompanyIdByCell(cell)
					InsertTime = ScanUtil.getTimeByCell(cell);
					DeviceId = ScanUtil.getDeviceIdByCell(cell);
					
					if(qulifier == 1) {
						EndTime = Bytes.toLong(CellUtil.cloneValue(cell));
					} else if(qulifier == 2) {
						Duration = Bytes.toLong(CellUtil.cloneValue(cell));
					} else if(qulifier == 3) {
						Currentvalue = Bytes.toFloat(CellUtil.cloneValue(cell));
					} else if(qulifier == 4) {
						Flag = CellUtil.cloneValue(cell)[0];
					} else if(qulifier == 5) {
						EventType = CellUtil.cloneValue(cell)[0];
						isOld = true;
					} 
				}
				if(isOld || EventType == 0) {
					dels.add(new Delete(Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(DeviceId), Bytes.toBytes(InsertTime))));
				}
			}
			HTableInterface htable =  HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_alarmdata_name"));
			for(Delete del : dels) {
				htable.delete(del);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
