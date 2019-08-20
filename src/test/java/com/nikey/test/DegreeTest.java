package com.nikey.test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.nikey.hbase.HbaseTablePool;
import com.nikey.util.JsonUtil;
import com.nikey.util.ScanUtil;

public class DegreeTest {
	
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
		private Short eventType;
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
		public Short getEventType() {
			return eventType;
		}
		public void setEventType(Short eventType) {
			this.eventType = eventType;
		}
	}
	
	public static void main(String[] args) throws ParseException, IOException {
		DegreeTest key = new DegreeTest();
		short companyId = Short.valueOf(args[0]);
		short deviceId = Short.valueOf(args[1]);
		key.scanKey(companyId, deviceId, args[2], args[3]);
	}
	
	public void scanKey(short CompanyId, short DeviceId, String startTime, String endTime) throws ParseException, IOException {
		HTableInterface htable =  HbaseTablePool.instance().getHtable("degreevalue");
				
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
			for (Result result : rScaner) {
	        	Degree degree = null;
	        	for(Cell cell : result.rawCells()) {
	        		int qualifier = CellUtil.cloneQualifier(cell)[0];
	        		if(qualifier == 1) {
	        			degree = new Degree();
	        			degree.setInsertTime(ScanUtil.getTimeByCell(cell));
	        			degree.setEventType(ScanUtil.getEventTypeByCell(cell));
	        			degree.setPeakEpi(Bytes.toDouble(CellUtil.cloneValue(cell)));
	        		} else if(qualifier == 2) {
	        			degree.setFlatEpi(Bytes.toDouble(CellUtil.cloneValue(cell)));
	        		} else if(qualifier == 3) {
	        			degree.setValleyEpi(Bytes.toDouble(CellUtil.cloneValue(cell)));
	        		} else if(qualifier == 4) {
	        			degree.setTotalEpi(Bytes.toDouble(CellUtil.cloneValue(cell)));
	        		} else if(qualifier == 5) {
	        			degree.setTotalEpo(Bytes.toDouble(CellUtil.cloneValue(cell)));
	        		} else if(qualifier == 6) {
	        			degree.setTotalEqind(Bytes.toDouble(CellUtil.cloneValue(cell)));
	        		} else if(qualifier == 7) {
	        			degree.setTotalEqcap(Bytes.toDouble(CellUtil.cloneValue(cell)));
	        		} else if(qualifier == 8) {
	        			degree.setPowerFactor(Bytes.toFloat(CellUtil.cloneValue(cell)));
	        		}
	        	}
	        	if(degree != null && degree.nullCheck()) {
	        		System.out.println(JsonUtil.toJson(degree));
	        	}
	        }
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
