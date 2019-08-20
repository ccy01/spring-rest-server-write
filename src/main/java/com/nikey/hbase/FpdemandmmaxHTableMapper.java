package com.nikey.hbase;

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

public class FpdemandmmaxHTableMapper implements HTableMapper {
    
    /**
     * slfj
     */
    Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public List<Put> convertParameterMapToPut(Map<String, String[]> request) {
        short CompanyId = 0;
        int DeviceId = 0;
        long InsertTime = 0l;
        try {
            CompanyId = NumberUtil.Short_valueOf(request, "CompanyId");
            DeviceId = NumberUtil.Int_valueOf(request, "DeviceId");
            String FPdemandMMaxTimeStr = String.valueOf(request.get("FPdemandMMaxTime")[0]);
            InsertTime = DateUtil.parseHHMMSSToDate(FPdemandMMaxTimeStr).getTime();// 毫秒级别
        } catch (Exception e) {
            // 数据畸变，返回null
            logger.error(e.getMessage());
            return null;
        }
        
        StringBuffer sb = new StringBuffer();
        sb.append("point2:CompanyId:" + CompanyId + ",DeviceId:" + DeviceId + ",InsertTime:"
                + DateUtil.formatToHHMMSS(InsertTime) + "\n");
        logger.info("consuming : FPdemandMMaxHTable, " + DeviceId + ", " + DateUtil.formatToHHMMSS(InsertTime));// 已被转换为毫秒级别

        Put put;

        /**
         * 若监测点大于30000，rowkey：companyId(short) + deviceId(short) + insertTime(long);eg,35001: 35+1+insertTime
         * 若监测点小于30000，rowkey：companyId(short) + deviceId(short) + insertTime(long);eg,25001: 25+25001+insertTime
         */
        if (DeviceId > 30000) {
            short _deviceId = (short) (DeviceId % 1000);
            put = new Put(Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(_deviceId), Bytes.toBytes(InsertTime)));
        } else {
            short _deviceId = (short) DeviceId;
            put = new Put(Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(_deviceId), Bytes.toBytes(InsertTime)));
        }

        addDemand(request, put);
        List<Put> puts = new ArrayList<Put>();
        puts.add(put);
        return puts;
        
    }
    
    private void addDemand(Map<String, String[]> request, Put put) {
        // WARN HOKO传过来的数据为kW
        float FPdemandMMax = NumberUtil.Float_valueOf(request, "FPdemandMMax");
        String FPdemandMMaxTimeStr = String.valueOf(request.get("FPdemandMMaxTime")[0]);
        Long FPdemandMMaxTime = DateUtil.parseHHMMSSToDate(FPdemandMMaxTimeStr).getTime();
        
        // WARN 历史遗留问题，存放到Hbase的需量单位为W  new byte[] { 24 }的值24为qualifier设定的值
        put.add(Bytes.toBytes("P"), new byte[] { 24 }, 256L, Bytes.toBytes(FPdemandMMax * 1000f));
        put.add(Bytes.toBytes("P"), new byte[] { 28 }, 256L, Bytes.toBytes(FPdemandMMaxTime));
    }
    
    private ThreadLocal<HTableInterface> htables = new ThreadLocal<HTableInterface>();

    @Override
    public boolean put(List<Put> put) {
        if (htables.get() == null) {
            try {
                htables.set(HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_monitordata_name")));
            } catch (IOException e) {
                logger.error(e.getMessage());
                return false;
            }
        }
        final HTableInterface htable = htables.get();

        try {
            // final long start = System.currentTimeMillis();
            htable.put(put);
            htable.flushCommits();// write to hbase immediately
            htable.close();
            // logger.info("put to hbase success..." + (System.currentTimeMillis() -
            // start));
            return true;
        } catch (Exception e) {
            logger.error(e.getMessage());
            return false;
        }
    }

    @Override
    public ResultScanner getResultScannerWithParameterMap(Scan scan) {
        HTableInterface htable;
        try {
            htable = HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_monitordata_name"));
            try {
                ResultScanner rScaner = htable.getScanner(scan);
                return rScaner;
            } catch (IOException e) {
                return null;
            }
        } catch (IOException e) {
            return null;
        }
    }

}
