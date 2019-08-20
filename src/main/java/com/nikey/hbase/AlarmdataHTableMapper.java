package com.nikey.hbase;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nikey.bean.Abnormaldata;
import com.nikey.util.DataFormatUtil;
import com.nikey.util.DateUtil;
import com.nikey.util.HostNameUtil;
import com.nikey.util.NumberUtil;
import com.nikey.util.PropUtil;
import com.nikey.util.ServiceHelper;

/**
 * @author jayzee
 * @date 28 Sep, 2014
 *	Alarmdata HTableMapper
 */
public class AlarmdataHTableMapper implements HTableMapper{
	
	/**
	 * slfj
	 */
	Logger logger = LoggerFactory.getLogger(getClass());
	
	private ThreadLocal<HTableInterface> htables = new ThreadLocal<HTableInterface>();
	
	public static void main(String[] args) {
	    Abnormaldata abnormaldata = new Abnormaldata();
	    Put put = new Put(Bytes.toBytes(0));
	    AlarmdataHTableMapper mm = new AlarmdataHTableMapper();
	    abnormaldata.setEndLong(0l);
	    abnormaldata.setStartLong(0l);
	    abnormaldata.setEventType(22005);
	    abnormaldata.setValue0(0f);
	    abnormaldata.setValue1(0f);
	    abnormaldata.setValue2(0f);
        int result = mm.convertAlarmdataToPut(abnormaldata, put);
        System.out.println(result);
    }
	
	@Override
	public List<Put> convertParameterMapToPut(Map<String, String[]> request) {
		short CompanyId = 0;
		short DeviceId = 0;
		long InsertTime = 0l;
		try {
			CompanyId = NumberUtil.Short_valueOf(request, "CompanyId");
			DeviceId = NumberUtil.Short_valueOf(request, "DeviceId");
			InsertTime = NumberUtil.Long_valueOf(request, "HappenTime");
			InsertTime = InsertTime * 1000l; // 放大为毫秒值
		} catch (Exception e) {
			// 数据畸变，返回null
			e.printStackTrace();
			return null;
		}
		
		short EventType = NumberUtil.Short_valueOf(request, "EventType");
		float Value0=NumberUtil.Float_valueOf(request, "Value0");
		float Value1=NumberUtil.Float_valueOf(request, "Value1");
		float Value2=NumberUtil.Float_valueOf(request, "Value2");
		
		long EndTime = NumberUtil.Long_valueOf(request, "EndTime");
		EndTime = EndTime * 1000l; // 放大为毫秒值
		float Duration = 0;
		if(EndTime != 0) {
			Duration = (EndTime-InsertTime)/1000;
		}
		String Currentvalue =null;
		int index=0;
		if(Value0!=-10000)index++;
        if(Value1!=-10000)index++;
        if(Value2!=-10000)index++;
        
        Abnormaldata abnormaldata = new Abnormaldata();
        abnormaldata.setValue0(Value0);
        abnormaldata.setValue1(Value1);
        abnormaldata.setValue2(Value2);
        abnormaldata.setStartLong(InsertTime);
        abnormaldata.setEndLong(EndTime);
        
        Value0=DataFormatUtil.formatFloatTwoBits(Value0);
        Value1=DataFormatUtil.formatFloatTwoBits(Value1);
        Value2=DataFormatUtil.formatFloatTwoBits(Value2);
        if(index==0){
            Currentvalue=null;
        }
        else{
            if(EventType==21001){
                Map<String,Object> monitoMap=ServiceHelper.instance().getMonitordataService().getRelationMap(DeviceId);
                if(monitoMap!=null){
                    double standU=(double) monitoMap.get("standarnU");
                    if(standU!=0){
                        double rate0=(Value0-standU)/standU*100;
                        rate0=DataFormatUtil.formatDoubleTwoBits(rate0);
                        double rate1=(Value1-standU)/standU*100;
                        rate1=DataFormatUtil.formatDoubleTwoBits(rate1);
                        double rate2=(Value2-standU)/standU*100;
                        rate2=DataFormatUtil.formatDoubleTwoBits(rate2);
                        Currentvalue="A:"+rate0+"%,B:"+rate1+"%,C:"+rate2+"%";
                    }
                }
            }
            else if(EventType==11001||EventType==11002||EventType==11005||EventType==22002||EventType==22003){
                if(index==3){
                    Currentvalue="A:"+Value0+"V,B:"+Value1+"V,C:"+Value2+"V";
                }
                else if(index==2){
                    Currentvalue=Value0+"V,"+Value1+"V";
                }
                else{
                    Currentvalue=Value0+"V";
                }
            }
            else if(EventType==11003||EventType==21002){
                if(index==3){
                    Currentvalue="A:"+Value0+"A,B:"+Value1+"A,C:"+Value2+"A";
                }
                else if(index==2){
                    Currentvalue=Value0+"A,"+Value1+"A";
                }
                else{
                    Currentvalue=Value0+"A";
                }
            }
            else if(EventType==11004||EventType==21003){
                String unit=null;
                String istransfomer=ServiceHelper.instance().getTransFormerInfoService().isTransFormerById(DeviceId);
                if(istransfomer!=null){
                    if(istransfomer.equals("0")){
                        unit="kW";
                    }
                    else{
                        unit="kVA";
                    }
                }
                if(unit!=null){
                    if(index==3){
                        Currentvalue="A:"+Value0+unit+",B:"+Value1+unit+",C:"+Value2+unit;
                    }
                    else if(index==2){
                        Currentvalue=Value0+unit+","+Value1+unit;
                    }
                    else{
                        Currentvalue=Value0+unit;
                    }
                }
            }
            else if(EventType==11006){
                if(index==3){
                    Currentvalue="A:"+Value0+"mA,B:"+Value1+"mA,C:"+Value2+"mA";
                }
                else if(index==2){
                    Currentvalue=Value0+"mA,"+Value1+"mA";
                }
                else{
                    Currentvalue=Value0+"mA";
                }
            }
            else if(EventType==22010){
                if(index==3){
                    Currentvalue="A:"+Value0+"Hz,B:"+Value1+"Hz,C:"+Value2+"Hz";
                }
                else if(index==2){
                    Currentvalue=Value0+"Hz,"+Value1+"Hz";
                }
                else{
                    Currentvalue=Value0+"Hz"; 
                }
            }
            else if(EventType==21004||EventType==21005||EventType==22001||EventType==22004||EventType==22005||EventType==11009){
                if(index==3){
                    Currentvalue="A:"+Value0+"%,B:"+Value1+"%,C:"+Value2+"%";
                }
                else if(index==2){
                    Currentvalue=Value0+"%,"+Value1+"%";
                }
                else{
                    Currentvalue=Value0+"%";
                }
            }
            else if(EventType==11007||EventType==11008||EventType==11011){
                if(index==3){
                    Currentvalue="A:"+Value0+"°C,B:"+Value1+"°C,C:"+Value2+"°C";
                }
                else if(index==2){
                    Currentvalue=Value0+"°C,"+Value1+"°C";
                }
                else{
                    Currentvalue=Value0+"°C";
                }
            }
            else if(EventType==11012||EventType==22008||EventType==22009){
                if(index==3){
                    Currentvalue="A:"+Value0+",B:"+Value1+",C:"+Value2;
                }
                else if(index==2){
                    Currentvalue=Value0+","+Value1;
                }
                else{
                    Currentvalue=Value0+"";
                }
            }
            else if(EventType==21006||EventType==21007||EventType==22011||EventType==22012){
                Currentvalue=null;
                if(Value0!=-10000){
                    Duration=Value0;
                }
            }
        }
//        terminalSettingsBackup(DeviceId,"point:EventType:" + EventType + ",InsertTime:" + DateUtil.formatToHHMMSS(InsertTime)+",Value0:"+Value0
//                +",Value1:"+Value1+",Value2:"+Value2+",Currentvalue:"+Currentvalue);

        abnormaldata.setDeviceId(DeviceId);
        abnormaldata.setStartTime(DateUtil.formatToHHMMSS(InsertTime));
        abnormaldata.setEventType(EventType);
        abnormaldata.setEndTime(DateUtil.formatToHHMMSS(EndTime));
        abnormaldata.setDuration(Duration);
        abnormaldata.setCurrenctValue(Currentvalue);

        Put put = new Put(Bytes.add(Bytes.toBytes(CompanyId),
                Bytes.toBytes(DeviceId),
                Bytes.toBytes(InsertTime)));
        int result = convertAlarmdataToPut(abnormaldata, put);
        if (! HostNameUtil.isAliyun()) {
			logger.info("consuming alarmdata x2");
		}
        ServiceHelper.instance().getAbnormalDataService().handleAlarmdata(abnormaldata, EndTime,InsertTime);
    
		
        /*// special handling
		if(EventType == 0 || EndTime == 0L) {
			return null;
		} else {
			String InsertTimeWave [] = request.get("InsertTimeWave");
			String CurrentvalueWave [] = request.get("CurrentvalueWave");
			if(InsertTimeWave != null && CurrentvalueWave != null && CurrentvalueWave.length == InsertTimeWave.length) {
				String param = null, errorMsg = null;
				switch(EventType) {
				case 11007:
					param = "P0";
					break;
				}
				String value = QualifierUtil.getString("monitordata_" + param);
				if(value != null) {
					String columnFamily = value.split(",")[0];
					byte qualifier = Byte.valueOf(value.split(",")[1]);
					final List<Put> puts = new ArrayList<Put>();
					for(int n=0; n<InsertTimeWave.length; n++) {
						long InsertTimeOfWave = Long.valueOf(InsertTimeWave[n]) * 1000; // 放大为毫秒
						float CurrentvalueOfWave = Float.valueOf(CurrentvalueWave[n]);
						if(InsertTimeOfWave == 0) {
							logger.info("length of wave : " + n);
							break;
						} else {
							Put putOfWave = new Put(Bytes.add(Bytes.toBytes(CompanyId),
									Bytes.toBytes(DeviceId),
									Bytes.toBytes(InsertTimeOfWave)));
							putOfWave.add(Bytes.toBytes(columnFamily), new byte[]{qualifier}, 0, Bytes.toBytes(CurrentvalueOfWave));
							puts.add(putOfWave);
						}
					}
					if(puts.size() > 0) {
						// 加入hbase && 超时打印
						try {
							final HTableInterface monitordataMapper = HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_monitordata_name"));
							Callable<String> task = new Callable<String>() {
								@Override
								public String call() throws Exception {
									monitordataMapper.put(puts);
									return "done";
								}
							};
							JobControllUtil.submitJob(task, 
									"deviceID is " + DeviceId + 
									", eventType is " + EventType +
									", insertTimeWave is " + JsonUtil.toJson(InsertTimeWave) +
									", currentValueWave is " + JsonUtil.toJson(CurrentvalueWave) + ".", 
									getClass().getSimpleName());
						} catch (IOException e) {
							errorMsg = e.getMessage();
						}
					}
				} else {
					errorMsg = "Alarmdata wave qualifier error";
				}
				if(errorMsg != null) {
					LogJsonUtil.errorJsonFileRecord(
							getClass().getSimpleName(),
							errorMsg,
							"deviceID is " + DeviceId + 
							", eventType is " + EventType +
							", insertTimeWave is " + JsonUtil.toJson(InsertTimeWave) +
							", currentValueWave is " + JsonUtil.toJson(CurrentvalueWave) + ".");
				}
			}
		}
		
		byte[] FaQualifier = { 1, 2, 3, 4 };
		
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[0]}, 0,
				Bytes.toBytes(EndTime));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[1]}, 0,
				Bytes.toBytes(Duration));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[2]}, 0,
				Bytes.toBytes(Currentvalue));
		put.add(Bytes.toBytes("A"), new byte[]{FaQualifier[3]}, 0,
				new byte[]{Flag});
		terminalSettingsBackup(DeviceId, "point2:CompanyId:" + CompanyId + ",DeviceId:" + DeviceId + ",InsertTime:" + DateUtil.formatToHHMMSS(InsertTime));*/
		
        List<Put> puts = new ArrayList<Put>();
		if(result == 0) {
            puts.add(put);
            return null;
		} else {
		    return null;
		}
	}

	/**
	 * @param abnormaldata
	 * @param put
	 * @return 0：成功，1：失败
	 */
	private int convertAlarmdataToPut(Abnormaldata abnormaldata, Put put) {
	    int result = 0;
	    if(abnormaldata.getEndLong() == 0l) { // 忽略结束事件，否则同一条越限数据写入HBbase多次
	        switch (abnormaldata.getEventType()) {
                case 11001:
                case 11002:
                case 11005: {
                    if(abnormaldata.getValue0() != -10000 &&
                        abnormaldata.getValue1() != -10000 &&
                        abnormaldata.getValue2() != -10000) {
                        put.add(Bytes.toBytes("U"), new byte[]{ 1 }, 0,
                                Bytes.toBytes(abnormaldata.getValue0()));
                        put.add(Bytes.toBytes("U"), new byte[]{ 2 }, 0,
                                Bytes.toBytes(abnormaldata.getValue1()));
                        put.add(Bytes.toBytes("U"), new byte[]{ 3 }, 0,
                                Bytes.toBytes(abnormaldata.getValue2()));
//                        logger.info(JsonUtil.toJson(abnormaldata));
                    }
                    break;
                }
                case 21002:
                case 11003: {
                    if(abnormaldata.getValue0() != -10000 &&
                        abnormaldata.getValue1() != -10000 &&
                        abnormaldata.getValue2() != -10000) {
                        put.add(Bytes.toBytes("I"), new byte[]{ 1 }, 0,
                                Bytes.toBytes(abnormaldata.getValue0()));
                        put.add(Bytes.toBytes("I"), new byte[]{ 2 }, 0,
                                Bytes.toBytes(abnormaldata.getValue1()));
                        put.add(Bytes.toBytes("I"), new byte[]{ 3 }, 0,
                                Bytes.toBytes(abnormaldata.getValue2()));
//                        logger.info(JsonUtil.toJson(abnormaldata));
                    }
                    break;
                }
                case 11006: {
                    if(abnormaldata.getValue0() != -10000) {
                        put.add(Bytes.toBytes("I"), new byte[]{ 19 }, 0,
                                Bytes.toBytes(abnormaldata.getValue0()));
//                        logger.info(JsonUtil.toJson(abnormaldata));
                    }
                    break;
                }
                default: {
                    result = 1;
                    break;
                }
            }
	    } else {
	        result = 1;
	    }
	    return result;
    }

    @Override
	public boolean put(final List<Put> put) {		
		if(htables.get() == null) {
			try {
				htables.set(HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_monitordata_name")));
			} catch (IOException e) {
				e.printStackTrace();
				return false;
			}
		}
		final HTableInterface htable = htables.get();		
		try {
			//final long start = System.currentTimeMillis();
			if(put != null && put.size() > 0) {
			    htable.put(put);
	            htable.flushCommits();// write to hbase immediately
	            htable.close();
			}
			//logger.info("put to hbase success..." + (System.currentTimeMillis() - start));
			return true;
		} catch (Exception e) {
		    e.printStackTrace();
			logger.info("put to hbase failure...");
			return false;
		}
	}

	@Override
	public ResultScanner getResultScannerWithParameterMap(Scan scan) {
		HTableInterface htable;
		try {
			htable = HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_alarmdata_name"));
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
	
	/**
	 * backup the terminal settings to local file
	 * @param deviceid
	 * @param backupstr
	 */
	@SuppressWarnings("unused")
    private void terminalSettingsBackup(Short deviceid, String backupstr) {
		File directory = new File("");//设定为当前文件夹
		try{
			String folder = directory.getAbsolutePath();
			String path = folder + File.separator + "backup" + File.separator + "alarmdata" + File.separator 
					+ deviceid + ".txt";
			
			File file = new File(path);
			if(!file.getParentFile().exists()) {
			    file.getParentFile().mkdirs();
			}
			if(!file.exists()) { 
				file.createNewFile();
			}
			if(file.exists()) {
				FileWriter writer = new FileWriter(file, true);
				writer.write(backupstr + "\n");
				writer.flush();
				writer.close();
				
			}
		}catch(Exception e){}
	}

}
