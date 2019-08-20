package com.nikey.hbase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nikey.service.AbnormalDataService;
import com.nikey.util.DateUtil;
import com.nikey.util.JsonUtil;
import com.nikey.util.PropUtil;
import com.nikey.util.ServiceHelper;

/**
 * @author jayzee
 * @date 28 Sep, 2014
 *	alarmwave HTableMapper
 */
public class JsonHTableMapper implements HTableMapper {
	
	/**
	 * slfj
	 */
	Logger logger = LoggerFactory.getLogger(getClass());
	
	@SuppressWarnings("unchecked")
    @Override
	public List<Put> convertParameterMapToPut(Map<String, String[]> request) {
	    List<Put> puts = null;
	    String[] json = request.get("json");
	    Map<String, Object> jsonMap = null;
	    if (json != null) {
	        try {
	            jsonMap = JsonUtil.fromJsonToHashMap(json[0]);
            } catch (Exception e) {
                logger.info("parse json error, the json string is : " + json[0]);
                e.printStackTrace();
            }
	    }
	    if (jsonMap == null || jsonMap.get("data_type") == null) {
	        return null;
	    }
	    if ("alarm_wave_v2".equals(jsonMap.get("data_type").toString())) {
	        // rowkey
	        short DeviceId = Double.valueOf(jsonMap.get("device_id").toString()).shortValue();
	        short CompanyId = (short) (DeviceId / 1000);
	        long InsertTime = Double.valueOf(jsonMap.get("happen_time").toString()).longValue() * 1000l;
            long HappenTime = InsertTime;
            float SpaceTime = Double.valueOf(jsonMap.get("space_time").toString()).floatValue();
            Put put = new Put(Bytes.add(Bytes.toBytes(CompanyId),
                    Bytes.toBytes(DeviceId),
                    Bytes.toBytes(HappenTime)));
	        logger.info(String.format("consuming %d %s", DeviceId, DateUtil.formatToHHMMSS(InsertTime)));
	        
	        // 仅筛选以下4类录波事件
	        String EventTypeStr = String.valueOf(jsonMap.get("cause").toString());
	        String[] EventTypeStrArr = EventTypeStr.split(",");
	        List<Byte> EventTypeList = new ArrayList<Byte>();
	        if (EventTypeStrArr != null) {
	            for (String temp : EventTypeStrArr) {
	                if (temp != null) {
	                    if ("22011".equals(temp)) {
	                        EventTypeList.add((byte) 1);
	                    } else if("22012".equals(temp)) {
	                        EventTypeList.add((byte) 100);
	                    } else if("21006".equals(temp)) { // 升
                            EventTypeList.add((byte) 200);
                        } else if("21007".equals(temp)) { // 降
                            EventTypeList.add((byte) 400);
                        }
	                }
	            }
	        }
	        byte EventType[] = new byte[EventTypeList.size()];
	        for(int in = 0; in<EventTypeList.size(); in++) {
	            EventType[in] = EventTypeList.get(in);
	        }
	        
	        // wave_array
	        List<Double> wave_list = (List<Double>) jsonMap.get("float_array");
	        ByteBuffer UaWaveBuf = ByteBuffer.allocate(1450 * 4);
	        ByteBuffer UbWaveBuf = ByteBuffer.allocate(1450 * 4);
	        ByteBuffer UcWaveBuf = ByteBuffer.allocate(1450 * 4);
	        ByteBuffer IaWaveBuf = ByteBuffer.allocate(1450 * 4);
	        ByteBuffer IbWaveBuf = ByteBuffer.allocate(1450 * 4);
	        ByteBuffer IcWaveBuf = ByteBuffer.allocate(1450 * 4);
	        // A段：80*6
	        exactWaveJson(wave_list, UaWaveBuf, UbWaveBuf, UcWaveBuf, 
	                IaWaveBuf, IbWaveBuf, IcWaveBuf, 0, 80);
	        // B段：520*6，80+520=600
            exactWaveJson(wave_list, UaWaveBuf, UbWaveBuf, UcWaveBuf, 
                    IaWaveBuf, IbWaveBuf, IcWaveBuf, 80*6, 520);
            // C段：50*6，80+520+50=650
            exactWaveJson(wave_list, UaWaveBuf, UbWaveBuf, UcWaveBuf, 
                    IaWaveBuf, IbWaveBuf, IcWaveBuf, 600*6, 50);
            // D段：200*6，80+520+50+200=850
            exactWaveJson(wave_list, UaWaveBuf, UbWaveBuf, UcWaveBuf, 
                    IaWaveBuf, IbWaveBuf, IcWaveBuf, 650*6, 200);
            // E段：600*6，80+520+50+200+600=1450
            exactWaveJson(wave_list, UaWaveBuf, UbWaveBuf, UcWaveBuf, 
                    IaWaveBuf, IbWaveBuf, IcWaveBuf, 850*6, 600);
            
            // 设置到put对象
	        byte[] FaQualifier = { 1, 2, 3, 4, 5, 6, 7, 8, 9};
	        put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[0]}, 0,
	                EventType);
	        put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[1]}, 0,
	                Bytes.toBytes(HappenTime));
	        put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[2]}, 0,
	                Bytes.toBytes(SpaceTime));
	        put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[3]}, 0,
	                Bytes.toBytes(UaWaveBuf));
	        put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[4]}, 0,
	                Bytes.toBytes(UbWaveBuf));
	        put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[5]}, 0,
	                Bytes.toBytes(UcWaveBuf));
	        put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[6]}, 0,
	                Bytes.toBytes(IaWaveBuf));
	        put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[7]}, 0,
	                Bytes.toBytes(IbWaveBuf));
	        put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[8]}, 0,
	                Bytes.toBytes(IcWaveBuf));
	        puts = new ArrayList<Put>();
	        put.setId("alarm_wave_v2");
	        puts.add(put);
	    } else if("degree_unit".equals(jsonMap.get("data_type").toString())) {
	    	logger.info(JsonUtil.toJson(jsonMap));
	    	double device = (double) jsonMap.get("device_id");
	    	int deviceId = (int) device;
	    	double isG = (double) jsonMap.get("is_group");
	    	int isGroup = (int) isG;
			double changeT = (double) jsonMap.get("insert_time") * 1000;
			long changeTime = (long) changeT;
			double epi = (double) jsonMap.get("epi");
			int operationType = 0;
			ServiceHelper service = ServiceHelper.instance();
			AbnormalDataService ab = service.getAbnormalDataService();
			ab.insertCraftEpi(deviceId, DateUtil.formatToHHMMSS(changeTime), epi,operationType, isGroup);
			puts = new ArrayList<Put>();
		} else if ("water_motor_temp".equals(jsonMap.get("data_type").toString())) {
			short DeviceId = Double.valueOf(jsonMap.get("device_id").toString()).shortValue();
	        short CompanyId = (short) (DeviceId / 1000);
	        long InsertTime = Double.valueOf(jsonMap.get("insert_time").toString()).longValue() * 1000l;
            float head_da = Double.valueOf(jsonMap.get("head_da").toString()).floatValue();
            float tail_da = Double.valueOf(jsonMap.get("tail_da").toString()).floatValue();
            float head_db = Double.valueOf(jsonMap.get("head_db").toString()).floatValue();
            float tail_db = Double.valueOf(jsonMap.get("tail_db").toString()).floatValue();
            float head_dc = Double.valueOf(jsonMap.get("head_dc").toString()).floatValue();
            float tail_dc = Double.valueOf(jsonMap.get("tail_dc").toString()).floatValue();
            float front_motor = Double.valueOf(jsonMap.get("front_motor").toString()).floatValue();
            float back_motor = Double.valueOf(jsonMap.get("back_motor").toString()).floatValue();
            float front_pump = Double.valueOf(jsonMap.get("front_pump").toString()).floatValue();
            float back_pump = Double.valueOf(jsonMap.get("back_pump").toString()).floatValue();
            
            Put put = new Put(Bytes.add(Bytes.toBytes(CompanyId),
                    Bytes.toBytes(DeviceId),
                    Bytes.toBytes(InsertTime)));
            // 设置到put对象
	        byte[] FaQualifier = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
	        put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[0]}, 0,
	                Bytes.toBytes(InsertTime));
	        if (head_da > 0) {
	        	put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[1]}, 0,
		                Bytes.toBytes(head_da));
			}
	        if (tail_da > 0) {
	        	put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[2]}, 0,
		                Bytes.toBytes(tail_da));
			}
			if (head_db > 0) {
				put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[3]}, 0,
		                Bytes.toBytes(head_db));
			}
			if (tail_db > 0) {
				put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[4]}, 0,
		                Bytes.toBytes(tail_db));
			}
			if (head_dc > 0) {
				put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[5]}, 0,
		                Bytes.toBytes(head_dc));
			}
			if (tail_dc > 0) {
				put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[6]}, 0,
		                Bytes.toBytes(tail_dc));
			}
			if (front_motor > 0) {
				put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[7]}, 0,
		                Bytes.toBytes(front_motor));
			}
			if (back_motor > 0) {
				put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[8]}, 0,
		                Bytes.toBytes(back_motor));
			}
			if (front_pump > 0) {
				put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[9]}, 0,
		                Bytes.toBytes(front_pump));
			}
			if (back_pump > 0) {
				put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[10]}, 0,
		                Bytes.toBytes(back_pump));
			}
	        
	        puts = new ArrayList<Put>();
	        put.setId("water_motor_temp");
	        puts.add(put);
		} else {
			logger.error("unkown json {}", JsonUtil.toJson(jsonMap));
		}
		return puts;
	}

    /**
     * j是开始遍历的下标，共6通道，每通道的长度为length
     * [..ua..][..ub..][..uc..][..ia..][..ib..][..ic..]
     * @param wave_list
     * @param UaWaveBuf
     * @param UbWaveBuf
     * @param UcWaveBuf
     * @param IaWaveBuf
     * @param IbWaveBuf
     * @param IcWaveBuf
     * @param j
     * @param length
     */
    private void exactWaveJson(List<Double> wave_list, ByteBuffer UaWaveBuf,
            ByteBuffer UbWaveBuf, ByteBuffer UcWaveBuf, ByteBuffer IaWaveBuf,
            ByteBuffer IbWaveBuf, ByteBuffer IcWaveBuf, int j, int length) {
        int end = j + length; // start + length (pointer of ua's range)
        for (; j<end; j++) {
            UaWaveBuf.put(Bytes.toBytes(Float.valueOf(wave_list.get(j + length * 0).floatValue())));
            UbWaveBuf.put(Bytes.toBytes(Float.valueOf(wave_list.get(j + length * 1).floatValue())));
            UcWaveBuf.put(Bytes.toBytes(Float.valueOf(wave_list.get(j + length * 2).floatValue())));
            IaWaveBuf.put(Bytes.toBytes(Float.valueOf(wave_list.get(j + length * 3).floatValue())));
            IbWaveBuf.put(Bytes.toBytes(Float.valueOf(wave_list.get(j + length * 4).floatValue())));
            IcWaveBuf.put(Bytes.toBytes(Float.valueOf(wave_list.get(j + length * 5).floatValue())));
        }
    }

	@Override
	public boolean put(final List<Put> put) {		
		try {
		    Put pp = put.get(0);
		    if ("alarm_wave_v2".equals(pp.getId())) {
		        HTableInterface htable = HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_alarmwave_name"));
	            htable.put(put);
	            htable.flushCommits();// write to hbase immediately
	            htable.close();
	            return true;
		    } else if ("water_motor_temp".equals(pp.getId())) {
		    	HTableInterface htable = HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_motor_temp"));
	            htable.put(put);
	            htable.flushCommits();// write to hbase immediately
	            htable.close();
	            return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public ResultScanner getResultScannerWithParameterMap(Scan scan) {
		HTableInterface htable;
		try {
			htable = HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_alarmwave_name"));
			try {
				long start = System.currentTimeMillis();
				ResultScanner rScaner = htable.getScanner(scan);
				logger.info("get to hbase success..." + (System.currentTimeMillis() - start));
				return rScaner;
			} catch (IOException e) {
				return null;
			}
		} catch (IOException e) {
			return null;
		}
	}
	
}
