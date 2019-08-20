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
import com.nikey.util.NumberUtil;
import com.nikey.util.PropUtil;
import com.nikey.util.ServiceHelper;

public class GroupkilowHTableMapper implements HTableMapper {
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
			InsertTime = NumberUtil.Long_valueOf(request, "HappenTime");
			InsertTime = InsertTime * 1000l; // 放大为毫秒值
		} catch (Exception e) {
			// 数据畸变，返回null
			e.printStackTrace();
			return null;
		}
		
        long tsDegree = DateUtil.getTimestampByInsertTime(InsertTime);
        
        if (CompanyId == 0) {
        	CompanyId = ServiceHelper.instance().getGroupInfoService().getCompanyIdByGroudId(DeviceId);
        }
        if (CompanyId == 0) {
        	logger.error("GroupId {} no match companyId", DeviceId);
        	return null;
        }

        Put put = new Put(Bytes.add(Bytes.toBytes(CompanyId),
                Bytes.toBytes(DeviceId), Bytes.toBytes(InsertTime)));

        Double Epi = NumberUtil.Double_valueOf(request, "Epi");
        Double EQind = NumberUtil.Double_valueOf(request, "EQind");
        Double EQcap = NumberUtil.Double_valueOf(request, "EQcap");
        Double Peak = NumberUtil.Double_valueOf(request, "Peak");
        Double Flat = NumberUtil.Double_valueOf(request, "Flat");
        Double Valley = NumberUtil.Double_valueOf(request, "Valley");
        StringBuffer sb = new StringBuffer();
        sb.append("point:CompanyId:" + CompanyId + ",GroupId:" + DeviceId
                + ",InsertTime:" + DateUtil.formatToHHMMSS(InsertTime) + "\n");
        sb.append("Epi:" + Epi + "\n");
        sb.append("EQind" + EQind + "\n");
        sb.append("EQcap" + EQcap + "\n");
        sb.append("Peak" + Peak + "\n");
        sb.append("Flat" + Flat + "\n");
        sb.append("Valley" + Valley + "\n");
        byte[] FuQualifier = { 6, 7, 8, 9, 10, 11 };
        put.add(Bytes.toBytes("A"), new byte[] { FuQualifier[0] }, tsDegree,
                Bytes.toBytes(Epi));
        put.add(Bytes.toBytes("A"), new byte[] { FuQualifier[1] }, tsDegree,
                Bytes.toBytes(EQind));
        put.add(Bytes.toBytes("A"), new byte[] { FuQualifier[2] }, tsDegree,
                Bytes.toBytes(EQcap));
        put.add(Bytes.toBytes("A"), new byte[] { FuQualifier[3] }, tsDegree,
                Bytes.toBytes(Peak));
        put.add(Bytes.toBytes("A"), new byte[] { FuQualifier[4] }, tsDegree,
                Bytes.toBytes(Flat));
        put.add(Bytes.toBytes("A"), new byte[] { FuQualifier[5] }, tsDegree,
                Bytes.toBytes(Valley));
        // terminalSettingsBackup(DeviceId, "point:CompanyId:" + CompanyId +
        // ",GroupId:" + DeviceId + ",InsertTime:" +
        // DateUtil.formatToHHMMSS(InsertTime));
        // terminalSettingsBackup2(DeviceId, sb.toString());
        List<Put> puts = new ArrayList<Put>();
        puts.add(put);
        return puts;
    }

    @Override
    public boolean put(List<Put> put) {
        if (htables.get() == null) {
            try {
                htables.set(HbaseTablePool.instance().getHtable(
                        PropUtil.getString("hbase_group_name")));
            } catch (IOException e) {
            	e.printStackTrace();
                return false;
            }
        }
        final HTableInterface htable = htables.get();

        try {
            // final long start = System.currentTimeMillis();
            htable.put(put);
            htable.flushCommits();// write to hbase immediately
            htable.close();
            // logger.info("put to hbase success..." +
            // (System.currentTimeMillis() - start));
            return true;
        } catch (Exception e) {
        	e.printStackTrace();
            return false;
        }
    }

    @Override
    public ResultScanner getResultScannerWithParameterMap(Scan scan) {
        HTableInterface htable;
        try {
            htable = HbaseTablePool.instance().getHtable(
                    PropUtil.getString("hbase_group_name"));
            try {
                long start = System.currentTimeMillis();
                ResultScanner rScaner = htable.getScanner(scan);
                logger.info("get to hbase success..."
                        + (System.currentTimeMillis() - start));
                return rScaner;
            } catch (IOException e) {
                return null;
            }
        } catch (IOException e) {
            return null;
        }
    }

    private void terminalSettingsBackup(Short deviceid, String backupstr) {
        File directory = new File("");// 设定为当前文件夹
        try {
            String folder = directory.getAbsolutePath();
            String path = folder + File.separator + "backup" + File.separator
                    + "groupkilow" + File.separator + deviceid + ".txt";

            File file = new File(path);
            if (!file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }
            if (!file.exists()) {
                file.createNewFile();
            }
            if (file.exists()) {
                FileWriter writer = new FileWriter(file, true);
                writer.write(backupstr + "\n");
                writer.flush();
                writer.close();

            }
        } catch (Exception e) {
        }
    }

    private void terminalSettingsBackup2(Short deviceid, String backupstr) {
        File directory = new File("");// 设定为当前文件夹
        try {
            String folder = directory.getAbsolutePath();
            String path = folder + File.separator + "backup2" + File.separator
                    + "groupkilow" + File.separator + deviceid + ".txt";

            File file = new File(path);
            if (!file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }
            if (!file.exists()) {
                file.createNewFile();
            }
            if (file.exists()) {
                FileWriter writer = new FileWriter(file);
                writer.write(backupstr + "\n");
                writer.flush();
                writer.close();

            }
        } catch (Exception e) {
        }
    }

}
