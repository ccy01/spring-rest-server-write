package com.nikey.hbase;

import java.io.IOException;
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

import com.nikey.util.DateUtil;
import com.nikey.util.NumberUtil;
import com.nikey.util.PropUtil;
import com.nikey.util.ServiceHelper;

/**
 * @author jayzee
 * 整点行度
 */
public class MoxamonitordataHTableMapper implements HTableMapper{
	
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
		
		Put put = new Put(Bytes.add(Bytes.toBytes(CompanyId),
				Bytes.toBytes(DeviceId),
				Bytes.toBytes(InsertTime)));
		float Ua = NumberUtil.Float_valueOf(request, "Ua");
		float Ub = NumberUtil.Float_valueOf(request, "Ub");
		float Uc = NumberUtil.Float_valueOf(request, "Uc");
		float Ia = NumberUtil.Float_valueOf(request, "Ia");
		float Ib = NumberUtil.Float_valueOf(request, "Ib");
		float Ic = NumberUtil.Float_valueOf(request, "Ic");
		float I01 = NumberUtil.Float_valueOf(request, "I01");
		float Pa = NumberUtil.Float_valueOf(request, "Pa");
		float Pb = NumberUtil.Float_valueOf(request, "Pb");
		float Pc = NumberUtil.Float_valueOf(request, "Pc");
		float P0 = NumberUtil.Float_valueOf(request, "P0");
		float Qa = NumberUtil.Float_valueOf(request, "Qa");
		float Qb = NumberUtil.Float_valueOf(request, "Qb");
		float Qc = NumberUtil.Float_valueOf(request, "Qc");
		float Q0 = NumberUtil.Float_valueOf(request, "Q0");
		float Sa = NumberUtil.Float_valueOf(request, "Sa");
		float Sb = NumberUtil.Float_valueOf(request, "Sb");
		float Sc = NumberUtil.Float_valueOf(request, "Sc");
		float S0 = NumberUtil.Float_valueOf(request, "S0");
		float Ca = NumberUtil.Float_valueOf(request, "Ca");
		float Cb = NumberUtil.Float_valueOf(request, "Cb");
		float Cc = NumberUtil.Float_valueOf(request, "Cc");
		float PFT = NumberUtil.Float_valueOf(request, "PFT");
		float F = NumberUtil.Float_valueOf(request, "F");
		
		
		double TotalEpi = NumberUtil.Double_valueOf(request, "TotalEpi");
		double TotalEpo = NumberUtil.Double_valueOf(request, "TotalEpo");
		double TotalEQind = NumberUtil.Double_valueOf(request, "TotalEQind");
		double TotalEQcap = NumberUtil.Double_valueOf(request, "TotalEQcap");
		
/*		System.out.println("A相电压:"+Ua);
		System.out.println("B相电压:"+Ub);
		System.out.println("C相电压:"+Uc);
		System.out.println("A相电流:"+Ia);
		System.out.println("B相电流:"+Ib);
		System.out.println("C相电流:"+Ic);
		System.out.println("零序电流:"+I01);
		System.out.println("A相有功功率:"+Pa);
		System.out.println("B相有功功率:"+Pb);
		System.out.println("C相有功功率:"+Pc);
		System.out.println("总有功功率:"+P0);
		System.out.println("A相无功功率:"+Qa);
		System.out.println("B相无功功率:"+Qb);
		System.out.println("C相无功功率:"+Qc);
		System.out.println("总无功功率:"+Q0);
		System.out.println("A相视在功率:"+Sa);
		System.out.println("B相视在功率:"+Sb);
		System.out.println("C相视在功率:"+Sc);
		System.out.println("总视在功率:"+S0);
		System.out.println("A相功率因数:"+Ca);
		System.out.println("B相功率因数:"+Cb);
		System.out.println("C相功率因数:"+Cc);
		System.out.println("总功率因数:"+PFT);
		System.out.println("频率:"+F);
		System.out.println("TotalEpi:"+TotalEpi);
		System.out.println("TotalEpo"+TotalEpo);
		System.out.println("TotalEQind:"+TotalEQind);*/
		
		put.add(Bytes.toBytes("U"), new byte[]{(byte)1}, 0,
				Bytes.toBytes(Ua));
		put.add(Bytes.toBytes("U"), new byte[]{(byte)2}, 0,
				Bytes.toBytes(Ub));
		put.add(Bytes.toBytes("U"), new byte[]{(byte)3}, 0,
				Bytes.toBytes(Uc));
		put.add(Bytes.toBytes("I"), new byte[]{(byte)1}, 0,
				Bytes.toBytes(Ia));
		put.add(Bytes.toBytes("I"), new byte[]{(byte)2}, 0,
				Bytes.toBytes(Ib));
		put.add(Bytes.toBytes("I"), new byte[]{(byte)3}, 0,
				Bytes.toBytes(Ic));
		put.add(Bytes.toBytes("I"), new byte[]{(byte)8}, 0,
				Bytes.toBytes(I01));
		put.add(Bytes.toBytes("P"), new byte[]{(byte)1}, 0,
				Bytes.toBytes(Pa));
		put.add(Bytes.toBytes("P"), new byte[]{(byte)2}, 0,
				Bytes.toBytes(Pb));
		put.add(Bytes.toBytes("P"), new byte[]{(byte)3}, 0,
				Bytes.toBytes(Pc));
		put.add(Bytes.toBytes("P"), new byte[]{(byte)4}, 0,
				Bytes.toBytes(P0));
		put.add(Bytes.toBytes("P"), new byte[]{(byte)9}, 0,
				Bytes.toBytes(Qa));
		put.add(Bytes.toBytes("P"), new byte[]{(byte)10}, 0,
				Bytes.toBytes(Qb));
		put.add(Bytes.toBytes("P"), new byte[]{(byte)11}, 0,
				Bytes.toBytes(Qc));
		put.add(Bytes.toBytes("P"), new byte[]{(byte)12}, 0,
				Bytes.toBytes(Q0));
		put.add(Bytes.toBytes("P"), new byte[]{(byte)13}, 0,
				Bytes.toBytes(Sa));
		put.add(Bytes.toBytes("P"), new byte[]{(byte)14}, 0,
				Bytes.toBytes(Sb));
		put.add(Bytes.toBytes("P"), new byte[]{(byte)15}, 0,
				Bytes.toBytes(Sc));
		put.add(Bytes.toBytes("P"), new byte[]{(byte)16}, 0,
				Bytes.toBytes(S0));
		put.add(Bytes.toBytes("P"), new byte[]{(byte)17}, 0,
				Bytes.toBytes(Ca));
		put.add(Bytes.toBytes("P"), new byte[]{(byte)18}, 0,
				Bytes.toBytes(Cb));
		put.add(Bytes.toBytes("P"), new byte[]{(byte)19}, 0,
				Bytes.toBytes(Cc));
		put.add(Bytes.toBytes("P"), new byte[]{(byte)22}, 0,
				Bytes.toBytes(PFT));
		put.add(Bytes.toBytes("D"), new byte[]{(byte)11}, 0,
				Bytes.toBytes(F));
		put.add(Bytes.toBytes("D"), new byte[]{(byte)1}, 0,
				Bytes.toBytes(TotalEpi));
		put.add(Bytes.toBytes("D"), new byte[]{(byte)8}, 0,
				Bytes.toBytes(TotalEpo));
		put.add(Bytes.toBytes("D"), new byte[]{(byte)9}, 0,
				Bytes.toBytes(TotalEQind));
		put.add(Bytes.toBytes("D"), new byte[]{(byte)10}, 0,
				Bytes.toBytes(TotalEQcap));
		
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

}
