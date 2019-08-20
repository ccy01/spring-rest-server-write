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

/**
 * 汉光仪表写入group表
 * 
 * @author JayzeeZhang
 * @date 2018年1月31日
 */
public class Groupv2HTableMapper implements HTableMapper {

	private Logger logger = LoggerFactory.getLogger(getClass());
	private ThreadLocal<HTableInterface> htables = new ThreadLocal<HTableInterface>();

	@Override
	public List<Put> convertParameterMapToPut(Map<String, String[]> request) {
		short CompanyId = 0;
		short DeviceId = 0;
		long InsertTime = 0l;
		try {
			CompanyId = NumberUtil.Short_valueOf(request, "CompanyId");
			DeviceId = NumberUtil.Short_valueOf(request, "DeviceId");
			InsertTime = NumberUtil.Long_valueOf(request, "InsertTime") * 1000l;// 毫秒级别
		} catch (Exception e) {
			// 数据畸变，返回null
			logger.error(e.getMessage());
			return null;
		}
		
		Put put = new Put(Bytes.add(Bytes.toBytes(CompanyId), 
				Bytes.toBytes(DeviceId), Bytes.toBytes(InsertTime)));

		// Epi小于0非法
		double Epi = NumberUtil.Double_valueOf(request, "Epi");
		double PeakEpi = NumberUtil.Double_valueOf(request, "PeakEpi");
		double FlatEpi = NumberUtil.Double_valueOf(request, "FlatEpi");
		double ValleyEpi = NumberUtil.Double_valueOf(request, "ValleyEpi");
		double EQind = NumberUtil.Double_valueOf(request, "EQind");
        double EQcap = NumberUtil.Double_valueOf(request, "EQcap");
		if (Epi < 0 || PeakEpi < 0 || FlatEpi < 0 || ValleyEpi < 0) {
			return null;
		}
		
		// 行度数据
		byte[] FuQualifier = { 6, 7, 8, 9, 10, 11 };
        put.add(Bytes.toBytes("A"), new byte[] { FuQualifier[0] }, 0l,
                Bytes.toBytes(Epi));
        put.add(Bytes.toBytes("A"), new byte[] { FuQualifier[1] }, 0l,
                Bytes.toBytes(EQind));
        put.add(Bytes.toBytes("A"), new byte[] { FuQualifier[2] }, 0l,
                Bytes.toBytes(EQcap));
        put.add(Bytes.toBytes("A"), new byte[] { FuQualifier[3] }, 0l,
                Bytes.toBytes(PeakEpi));
        put.add(Bytes.toBytes("A"), new byte[] { FuQualifier[4] }, 0l,
                Bytes.toBytes(FlatEpi));
        put.add(Bytes.toBytes("A"), new byte[] { FuQualifier[5] }, 0l,
                Bytes.toBytes(ValleyEpi));
        
        // 功率数据
        float P0 = NumberUtil.Float_valueOf(request, "P0");
        float Q0 = NumberUtil.Float_valueOf(request, "Q0");
        float S0 = NumberUtil.Float_valueOf(request, "S0");
        FuQualifier = new byte[] { 1, 2, 3 ,12};
        put.add(Bytes.toBytes("A"), new byte[]{FuQualifier[0]}, 0,
                Bytes.toBytes(P0));
        put.add(Bytes.toBytes("A"), new byte[]{FuQualifier[1]}, 0,
                Bytes.toBytes(Q0));
        put.add(Bytes.toBytes("A"), new byte[]{FuQualifier[2]}, 0,
                Bytes.toBytes(S0));
        float PowerFactor = 0;
        if (NumberUtil.isZero(S0) || NumberUtil.isZero(P0)) {
        	PowerFactor = 1;
        } else {
        	PowerFactor = P0 / S0;
        }
        put.add(Bytes.toBytes("A"), new byte[]{FuQualifier[3]}, 0,
                Bytes.toBytes(PowerFactor));
        
        // 需量数据
        // TODO 历史遗留问题，存放到Hbase的需量单位为W
        float FPdemand = NumberUtil.Float_valueOf(request, "FPdemand");
        FuQualifier = new byte[] { 4, 5 };
        put.add(Bytes.toBytes("A"), new byte[]{ FuQualifier[0] }, 0, Bytes.toBytes(InsertTime));
        put.add(Bytes.toBytes("A"), new byte[]{ FuQualifier[1] }, 0, Bytes.toBytes(FPdemand * 1000f));

        logger.info(String.format("consuming groupv2, company %d, device %d, time %s", CompanyId, DeviceId, DateUtil.formatToHHMMSS(InsertTime)));
        
        List<Put> puts = new ArrayList<Put>();
        puts.add(put);
        return puts;
	}

	@Override
	public boolean put(List<Put> put) {
		if (htables.get() == null) {
			try {
				htables.set(HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_group_name")));
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

}
