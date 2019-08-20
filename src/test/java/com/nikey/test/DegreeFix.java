package com.nikey.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.nikey.hbase.HbaseTablePool;
import com.nikey.hbase.MonitordataHTableMapper;
import com.nikey.util.DateUtil;
import com.nikey.util.ScanUtil;

public class DegreeFix {
	
	short companyId = 1, startDevice = 1001, endDevice = 1024;
	byte searchType = 0;
	long offsetTime = 1000 * 60 * 5; // 数据偏移5分钟
	static HTableInterface htable = null;
	MonitordataHTableMapper mapper = new MonitordataHTableMapper();
	
	public static void main(String[] args) {
		try {
			htable = HbaseTablePool.instance().getHtable("degreevalue");
			DegreeFix fix = new DegreeFix();
			fix.degreeFix("2015-01-01 00:00:00", "2015-04-14 00:00:00");
			fix.searchType = 1;
			fix.degreeFix("2015-01-01 00:00:00", "2015-04-14 00:00:00");
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("happy ending...");
	}
	
	public void degreeFix(String starttime, String endtime) {
		
		long startTime = DateUtil.parseHHMMSSToDate(starttime).getTime();
		long endTime = DateUtil.parseHHMMSSToDate(endtime).getTime();
		long sub = 0;
		if(searchType == 0) {
			// hour
			sub = 1000 * 60 * 60;
		} else {
			// day
			sub = 1000 * 60 * 60 * 24;
		}
		
		for(short DeviceId = startDevice; DeviceId <= endDevice; DeviceId ++) {
			for(long time = startTime; time <= endTime; time += sub) {
				Get get = new Get(Bytes.add(
						Bytes.add(Bytes.toBytes(companyId),
								Bytes.toBytes(DeviceId),
								Bytes.toBytes(time)
							),
						new byte[]{searchType}));
				try {
					
					Result result = htable.get(get);
					boolean isEmpty = true;
					for (Cell cell : result.rawCells()) {
	                    byte qualifier = CellUtil.cloneQualifier(cell)[0];
	                    byte type = ScanUtil.getDegreeType(cell);
	                    if(qualifier == (byte)1 && type == searchType) {
	                    	isEmpty = false;
	                    	break;
	                    }
	                }
					if(isEmpty) {
//						System.out.println("data missing : " + DateUtil.formatToHHMMSS(time));
						// 向后推：2015-01-01 00:00:00的小时电度由[2015-01-01 00:00:00, 2015-01-01 01:00:00]产生
						Degree first = getDegree(DeviceId, time);
//						System.out.println("first : " + DateUtil.formatToHHMMSS(first.getInsertTime()));
						Degree last = getDegree(DeviceId, time + sub);
//						System.out.println("last : " + DateUtil.formatToHHMMSS(last.getInsertTime()));
						Put put = getPutByDegree(first, last, time, DeviceId);
						htable.put(put);
					}
					
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	
	private Put getPutByDegree(Degree first, Degree last, long time, short DeviceId) {
		if(first == null || last == null) {
			System.out.println("data missing : " + DeviceId + "," + DateUtil.formatToHHMMSS(time));
			Put put = new Put(
					Bytes.add(
							Bytes.add(Bytes.toBytes(companyId),
									Bytes.toBytes(DeviceId),
									Bytes.toBytes(time)
								),
							new byte[]{searchType})
				);
		
			double valueD = 0;
			float valueF = 0; 
			long timestamp = 4096l;
			byte[] FaQualifier = { 1, 2, 3, 4, 5, 6, 7, 8};
			put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[0]}, timestamp,
					Bytes.toBytes(valueD));
			put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[1]}, timestamp,
					Bytes.toBytes(valueD));
			put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[2]}, timestamp,
					Bytes.toBytes(valueD));
			put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[3]}, timestamp,
					Bytes.toBytes(valueD));
			put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[4]}, timestamp,
					Bytes.toBytes(valueD));
			put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[5]}, timestamp,
					Bytes.toBytes(valueD));
			put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[6]}, timestamp,
					Bytes.toBytes(valueD));
			put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[7]}, timestamp,
					Bytes.toBytes(valueF));			
			return put;
		} else {
			Degree degree = new Degree();
			degree.setInsertTime(time);
			degree.setFlatEpi(last.getFlatEpi() - first.getFlatEpi());
			degree.setPeakEpi(last.getPeakEpi() - first.getPeakEpi());
			degree.setValleyEpi(last.getValleyEpi() - first.getValleyEpi());
			degree.setTotalEpi(last.getTotalEpi() - first.getTotalEpi());
			degree.setTotalEpo(last.getTotalEpo() - first.getTotalEpo());
			degree.setTotalEqind(last.getTotalEqind() - first.getTotalEqind());
			degree.setTotalEqcap(last.getTotalEqcap() - first.getTotalEqcap());
			// 计算功率因数
			Float powerFactor = (float) (degree.getTotalEpi() / (Math.sqrt( Math.pow(degree.getTotalEpi(), 2) + Math.pow(degree.getTotalEqind(), 2) )));
			degree.setPowerFactor(powerFactor);
			
			Put put = new Put(
					Bytes.add(
							Bytes.add(Bytes.toBytes(companyId),
									Bytes.toBytes(DeviceId),
									Bytes.toBytes(time)
								),
							new byte[]{searchType})
				);
		
		
			byte[] FaQualifier = { 1, 2, 3, 4, 5, 6, 7, 8};
			put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[0]}, 0,
					Bytes.toBytes(degree.getPeakEpi()));
			put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[1]}, 0,
					Bytes.toBytes(degree.getFlatEpi()));
			put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[2]}, 0,
					Bytes.toBytes(degree.getValleyEpi()));
			put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[3]}, 0,
					Bytes.toBytes(degree.getTotalEpi()));
			put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[4]}, 0,
					Bytes.toBytes(degree.getTotalEpo()));
			put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[5]}, 0,
					Bytes.toBytes(degree.getTotalEqind()));
			put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[6]}, 0,
					Bytes.toBytes(degree.getTotalEqcap()));
			put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[7]}, 0,
					Bytes.toBytes(degree.getPowerFactor()));
			
			return put;
		}
	}

	private Degree getDegree(short deviceId, long time) {
		Scan scan = new Scan();
        scan.setStartRow(Bytes.add(Bytes.toBytes(companyId),
                Bytes.toBytes(deviceId), Bytes.toBytes(time - offsetTime)));
        scan.setStopRow(Bytes.add(Bytes.toBytes(companyId),
                Bytes.toBytes(deviceId), Bytes.toBytes(time + offsetTime)));
        scan.addColumn(Bytes.toBytes("D"), new byte[]{(byte) 12});
        scan.addColumn(Bytes.toBytes("D"), new byte[]{(byte) 13});
        scan.addColumn(Bytes.toBytes("D"), new byte[]{(byte) 14});
        scan.addColumn(Bytes.toBytes("D"), new byte[]{(byte) 1});
        scan.addColumn(Bytes.toBytes("D"), new byte[]{(byte) 8});
        scan.addColumn(Bytes.toBytes("D"), new byte[]{(byte) 9});
        scan.addColumn(Bytes.toBytes("D"), new byte[]{(byte) 10});
        
        List<Degree> list = new ArrayList<DegreeFix.Degree>();
        ResultScanner scanner = mapper.getResultScannerWithParameterMap(scan);
        for(Result result : scanner) {
        	Degree degree = null;
        	for(Cell cell : result.rawCells()) {
        		int qualifier = CellUtil.cloneQualifier(cell)[0];
        		if(qualifier == 1) {
        			degree = new Degree();
        			degree.setInsertTime(ScanUtil.getTimeByCell(cell));
        			degree.setTotalEpi(Bytes.toDouble(CellUtil.cloneValue(cell)));
        		} else if(qualifier == 8) {
        			degree.setTotalEpo(Bytes.toDouble(CellUtil.cloneValue(cell)));
        		} else if(qualifier == 9) {
        			degree.setTotalEqind(Bytes.toDouble(CellUtil.cloneValue(cell)));
        		} else if(qualifier == 10) {
        			degree.setTotalEqcap(Bytes.toDouble(CellUtil.cloneValue(cell)));
        		} else if(qualifier == 12) {
        			degree.setPeakEpi(Bytes.toDouble(CellUtil.cloneValue(cell)));
        		} else if(qualifier == 13) {
        			degree.setFlatEpi(Bytes.toDouble(CellUtil.cloneValue(cell)));
        		} else if(qualifier == 14) {
        			degree.setValleyEpi(Bytes.toDouble(CellUtil.cloneValue(cell)));
        		}
        	}
        	if(degree != null && degree.nullCheck()) {
        		list.add(degree);
        	}
        }
        
        if(list.size() < 2) {
        	return null;
        } else {
        	return getNeareastDegree(list, time);
        }
	}


	private Degree getNeareastDegree(List<Degree> list, long time) {
		Long minSub = offsetTime;
		Degree degree = null;
		for(Degree item : list) {
			long sub = Math.abs(item.getInsertTime() - time);
			if(sub <= minSub) {
				minSub = sub;
				degree = item;
			}
		}
		return degree;
	}


	class Degree {
		private Double peakEpi;
		private Double flatEpi;
		private Double valleyEpi;
		private Double totalEpi;
		private Double totalEpo;
		private Double totalEqind;
		private Double totalEqcap;
		private Float powerFactor;
		private Long insertTime;
		public boolean nullCheck() {
			if(getPeakEpi() != null &&
					getFlatEpi() != null &&
					getValleyEpi() != null &&
					getTotalEpi() != null &&
					getTotalEpo() != null &&
					getTotalEqind() != null &&
					getTotalEqcap() != null) {
				return true;
			}
			else return false;
		}
		public Long getInsertTime() {
			return insertTime;
		}
		public void setInsertTime(Long insertTime) {
			this.insertTime = insertTime;
		}
		public Double getPeakEpi() {
			return peakEpi;
		}
		public void setPeakEpi(Double peakEpi) {
			this.peakEpi = peakEpi;
		}
		public Double getFlatEpi() {
			return flatEpi;
		}
		public void setFlatEpi(Double flatEpi) {
			this.flatEpi = flatEpi;
		}
		public Double getValleyEpi() {
			return valleyEpi;
		}
		public void setValleyEpi(Double valleyEpi) {
			this.valleyEpi = valleyEpi;
		}
		public Double getTotalEpi() {
			return totalEpi;
		}
		public void setTotalEpi(Double totalEpi) {
			this.totalEpi = totalEpi;
		}
		public Double getTotalEpo() {
			return totalEpo;
		}
		public void setTotalEpo(Double totalEpo) {
			this.totalEpo = totalEpo;
		}
		public Double getTotalEqind() {
			return totalEqind;
		}
		public void setTotalEqind(Double totalEqind) {
			this.totalEqind = totalEqind;
		}
		public Double getTotalEqcap() {
			return totalEqcap;
		}
		public void setTotalEqcap(Double totalEqcap) {
			this.totalEqcap = totalEqcap;
		}
		public Float getPowerFactor() {
			return powerFactor;
		}
		public void setPowerFactor(Float powerFactor) {
			this.powerFactor = powerFactor;
		}
	}

}
