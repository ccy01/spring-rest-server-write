package com.nikey.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nikey.util.NumberUtil;
import com.nikey.util.PropUtil;
import com.nikey.util.ServiceHelper;

public class Klm4134HTableMapper implements HTableMapper {
	
	Logger logger = LoggerFactory.getLogger(getClass());
	
	private ThreadLocal<HTableInterface> htables = new ThreadLocal<HTableInterface>();

	@Override
	public List<Put> convertParameterMapToPut(Map<String, String[]> request) {/*
		// rowkey
		short CompanyId = NumberUtil.Short_valueOf(request, "CompanyId");
		short DeviceId = NumberUtil.Short_valueOf(request, "DeviceId");
		long InsertTime = NumberUtil.Long_valueOf(request, "InsertTime");
		InsertTime = InsertTime * 1000l; // 放大为毫秒值
		
		Map<Short, String> relation = ServiceHelper.instance().getKlm4134Service().getRelationMap(DeviceId);
		if(relation != null) {
			float temperatureA = NumberUtil.Float_valueOf(request, "TemperatrueA");
			float temperatureB = NumberUtil.Float_valueOf(request, "TemperatrueB");
			float temperatureC = NumberUtil.Float_valueOf(request, "TemperatrueC");
			float temperatureD = NumberUtil.Float_valueOf(request, "TemperatrueD");
			
			List<Put> puts = new ArrayList<Put>();
			Set<Short> keySet = relation.keySet();
			for(Short RealDeviceID : keySet) {
				if( relation.get(RealDeviceID) != null && (!"".equals(relation.get(RealDeviceID))) ) {
					Put put = new Put(Bytes.add(Bytes.toBytes(CompanyId),
							Bytes.toBytes(RealDeviceID),
							Bytes.toBytes(InsertTime)));
					String arr [] = relation.get(RealDeviceID).split(",");
					for(String qualifer : arr) {
						switch (qualifer) {
						case "1":
							put.add(Bytes.toBytes("A"), new byte[]{Byte.valueOf(qualifer)}, 0,
									Bytes.toBytes(temperatureA));
							break;
						case "2":
							put.add(Bytes.toBytes("A"), new byte[]{Byte.valueOf(qualifer)}, 0,
									Bytes.toBytes(temperatureB));
							break;
						case "3":
							put.add(Bytes.toBytes("A"), new byte[]{Byte.valueOf(qualifer)}, 0,
									Bytes.toBytes(temperatureC));
							break;
						default: // 第四通道默认存储在temperatureD
							put.add(Bytes.toBytes("A"), new byte[]{Byte.valueOf(qualifer)}, 0,
									Bytes.toBytes(temperatureD));
							break;
						}
					}
					puts.add(put);
				}
			}
			return puts.size() != 0 ? puts : null;
		} else {
			return null;
		}
	*/
	    return null;}

	@Override
	public boolean put(List<Put> put) {		
		if(htables.get() == null) {
			try {
				htables.set(HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_temperature_name")));
			} catch (IOException e) {
				e.printStackTrace();
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
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} 
	}

	@Override
	public ResultScanner getResultScannerWithParameterMap(Scan scan) {
		// for write project, do nothing
		return null;
	}

}