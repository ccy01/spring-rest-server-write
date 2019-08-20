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

import com.nikey.util.DateUtil;
import com.nikey.util.JsonUtil;
import com.nikey.util.NumberUtil;
import com.nikey.util.PropUtil;

/**
 * @author jayzee
 * 整点行度
 */
public class KilowHTableMapper implements HTableMapper{
	
	/**
	 * slfj
	 */
	Logger logger = LoggerFactory.getLogger(getClass());
	
	private ThreadLocal<HTableInterface> htables = new ThreadLocal<HTableInterface>();
	
	@Override
	public List<Put> convertParameterMapToPut(Map<String, String[]> request) {
		short CompanyId = 0;
		short DeviceId = 0;
		long InsertTime = 0l;
		try {
			CompanyId = NumberUtil.Short_valueOf(request, "CompanyId");
			DeviceId = NumberUtil.Short_valueOf(request, "DeviceId");
			InsertTime = NumberUtil.Long_valueOf(request, "InsertTime");
			InsertTime = InsertTime * 1000l; // 放大为毫秒值
		} catch (Exception e) {
			// 数据畸变，返回null
			e.printStackTrace();
			return null;
		}
		
		long tsDegree = DateUtil.getTimestampByInsertTime(InsertTime);
		
		Put put = new Put(Bytes.add(Bytes.toBytes(CompanyId),
				Bytes.toBytes(DeviceId),
				Bytes.toBytes(InsertTime)));
		
		double Epi = NumberUtil.Double_valueOf(request, "Epi");
		double Epia = NumberUtil.Double_valueOf(request, "Epia");
		double Epib = NumberUtil.Double_valueOf(request, "Epib");
		double Epic = NumberUtil.Double_valueOf(request, "Epic");
		double Epoa = NumberUtil.Double_valueOf(request, "Epoa");
		double Epob = NumberUtil.Double_valueOf(request, "Epob");
		double Epoc = NumberUtil.Double_valueOf(request, "Epoc");
		double Epo = NumberUtil.Double_valueOf(request, "Epo");
		double EQind = NumberUtil.Double_valueOf(request, "EQind");
		double EQcap = NumberUtil.Double_valueOf(request, "EQcap");
		double PeakEpi = NumberUtil.Double_valueOf(request, "PeakEpi");
		double FlatEpi = NumberUtil.Double_valueOf(request, "FlatEpi");
		double ValleyEpi = NumberUtil.Double_valueOf(request, "ValleyEpi");
		
		// 1. epi小于0非法
		if (Epi < 0 || PeakEpi < 0 || FlatEpi < 0 || ValleyEpi < 0) {
			return null;
		}
		double sub = Math.abs(Epi - PeakEpi - FlatEpi - ValleyEpi);
		// 2. peak + flat + valley 不等于 epi 非法
		if (sub > 1) {
			return null;
		}

		/*Object[] params = ServiceHelper.instance().getKilowService().getRelationMap(CompanyId);
		
		if (params == null){//存入第一个epi
		    Date date =new Date();
		    String nowDateStr = DateUtil.formatToYYMMDD(date);
		    String insertDateStr = DateUtil.formatToYYMMDD(InsertTime);
		    if(nowDateStr.equals(insertDateStr)){//判断是否同一天
		        params = new Object[]{InsertTime,Epi};
	            ServiceHelper.instance().getKilowService().putrelationMap(CompanyId,params); 
		    }
		    else{//不是同一天
		        if(Epi < (Epi/769)){//判断极小值
                    return null;
                }
		    }
		}
		else{
		    long formerTime = (long) params[0];
		    double formerEpi = (double) params[1];
		    if(InsertTime > formerTime){//非重召类数据
		        if (formerEpi > Epi){//传入数据比当前数据小
		            return null;
		        }
		        else{
		            if(Epi > Epi*769){//判断极大值
		                return null;
		            }
		            else{
		                params = new Object[]{InsertTime,Epi};
		                ServiceHelper.instance().getKilowService().putrelationMap(CompanyId,params);
		            }
		        }
		    }
		    else{//重召类数据
		        if(Epi < (Epi/769)){//判断极小值
		            return null;
		        }
		    }
		}*/
		
		StringBuffer sb = new StringBuffer();
		sb.append("point2:CompanyId:" + CompanyId + ",DeviceId:" + DeviceId + ",InsertTime:" + DateUtil.formatToHHMMSS(InsertTime)+"\n");
		sb.append("Epi:"+Epi+"\n");
		sb.append("PeakEpi"+PeakEpi+"\n");
		sb.append("FlatEpi:"+FlatEpi+"\n");
		sb.append("ValleyEpi:"+ValleyEpi+"\n");
		
		byte[] FdQualifier = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
				15, 16, 17, 18, 19, 20, 21};
		put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[0]}, tsDegree,
				Bytes.toBytes(Epi));
		put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[1]}, tsDegree,
				Bytes.toBytes(Epia));
		put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[2]}, tsDegree,
				Bytes.toBytes(Epib));
		put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[3]}, tsDegree,
				Bytes.toBytes(Epic));
		put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[4]}, tsDegree,
				Bytes.toBytes(Epoa));
		put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[5]}, tsDegree,
				Bytes.toBytes(Epob));
		put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[6]}, tsDegree,
				Bytes.toBytes(Epoc));
		put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[7]}, tsDegree,
				Bytes.toBytes(Epo));
		put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[8]}, tsDegree,
				Bytes.toBytes(EQind));
		put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[9]}, tsDegree,
				Bytes.toBytes(EQcap));
		put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[11]}, tsDegree,
				Bytes.toBytes(PeakEpi));
		put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[12]}, tsDegree,
				Bytes.toBytes(FlatEpi));
		put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[13]}, tsDegree,
				Bytes.toBytes(ValleyEpi));
		
	    terminalSettingsBackup2(DeviceId, sb.toString());
		List<Put> puts = new ArrayList<Put>();
		puts.add(put);
		return puts;
	}

	@Override
	public boolean put(List<Put> put) {		
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
			htable.put(put);
			htable.flushCommits();// write to hbase immediately
			htable.close();
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} 
	}

	@Override
	public ResultScanner getResultScannerWithParameterMap(Scan scan) {
		return null;
	}

	
    private void terminalSettingsBackup(Short deviceid, String backupstr) {
        File directory = new File("");//设定为当前文件夹
        try{
            String folder = directory.getAbsolutePath();
            String path = folder + File.separator + "backup" + File.separator + "monitordata" + File.separator 
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
    
    private void terminalSettingsBackup2(Short deviceid, String backupstr) {
        File directory = new File("");//设定为当前文件夹
        try{
            String folder = directory.getAbsolutePath();
            String path = folder + File.separator + "backup2" + File.separator + "kilow" + File.separator 
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
