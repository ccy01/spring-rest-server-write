package com.nikey.hbase;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
import com.nikey.util.DataTransferUtil;

/**
 * @author jayzee
 * @date 25 Sep, 2014
 *	Monitordata HTable Mapper instance
 */
public class MonitordataHTableMapper implements HTableMapper {
	
	/**
	 * slfj
	 */
	Logger logger = LoggerFactory.getLogger(getClass());
	
	public static void main(String[] args) {
		System.out.println(1511849080l - (1511849080l % 3600));
		System.out.println(1511848800l % 3600);
	}
	
	private ThreadLocal<HTableInterface> htables = new ThreadLocal<HTableInterface>();
	
	private Map<Short, String> deviceIDAndFlickerMaps = new ConcurrentHashMap<Short, String>();
	
	class Voltage {
	    float ua;
	    float ub;
	    float uc;
	}
	
	class Current {
        float ia;
        float ib;
        float ic;
    }

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
		
		// 1. epi小于0非法
		double Epi = NumberUtil.Double_valueOf(request, "Epi");
		double PeakEpi = NumberUtil.Double_valueOf(request, "PeakEpi");
		double FlatEpi = NumberUtil.Double_valueOf(request, "FlatEpi");
		double ValleyEpi = NumberUtil.Double_valueOf(request, "ValleyEpi");
		if (Epi < 0 || PeakEpi < 0 || FlatEpi < 0 || ValleyEpi < 0) {
			return null;
		}
		double sub = Math.abs(Epi - PeakEpi - FlatEpi - ValleyEpi);
		// 2. peak + flat + valley 不等于 epi 非法
		if (sub > 1) {
			return null;
		}
		
		Put put = new Put(Bytes.add(Bytes.toBytes(CompanyId),
				Bytes.toBytes(DeviceId),
				Bytes.toBytes(InsertTime)));
		
		StringBuffer sb = new StringBuffer();
		sb.append("point2:CompanyId:" + CompanyId + ",DeviceId:" + DeviceId + ",InsertTime:" + DateUtil.formatToHHMMSS(InsertTime)+"\n");
		logger.info("consuming : monitordata, " + DeviceId + ", " + DateUtil.formatToHHMMSS(InsertTime));

		Voltage voltage = new Voltage();
		addFamilyU(put, request, sb, voltage);// monitortable columnFamily U
		Current current = new Current();
		addFamilyI(put, request, sb, current);// monitortable columnFamily I
		addFamilyP(put, request, sb, voltage, current);// monitortable columnFamily P
		addFamilyD(put, request, sb);// monitortable columnFamily D
		
		addFamilyXB(put, request, "R", "XBia", current.ia);// monitortable columnFamily R S T X Y Z 
		addFamilyXB(put, request, "S", "XBib", current.ib);
		addFamilyXB(put, request, "T", "XBic", current.ic);
		addFamilyXB(put, request, "X", "XBua", voltage.ua);
		addFamilyXB(put, request, "Y", "XBub", voltage.ub);
		addFamilyXB(put, request, "Z", "XBuc", voltage.uc);
		if (DeviceId == 12001){
//		    terminalSettingsBackup(DeviceId, "point2:CompanyId:" + CompanyId + ",DeviceId:" + DeviceId + ",InsertTime:" + DateUtil.formatToHHMMSS(InsertTime));
		    terminalSettingsBackup2(DeviceId, sb.toString());
		}
		
		// 需量独立为一个put
		byte[] FpQualifier = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
				15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28 };
		float FPdemand = NumberUtil.Float_valueOf(request, "FPdemand");
		float BPdemand = NumberUtil.Float_valueOf(request, "BPdemand");
		float FQdemand = NumberUtil.Float_valueOf(request, "FQdemand");
		float BQdemand = NumberUtil.Float_valueOf(request, "BQdemand");
		// TODO 与通信程序规约使用CumulativeTime表示需量时间
		long DemandTime = InsertTime;
		if (! "-1".equals(request.get("CumulativeTime")[0])) {
			DemandTime = NumberUtil.Long_valueOf(request, "CumulativeTime");
			DemandTime = DemandTime * 1000l; // 放大为毫秒值
		}
		Put putDemand = new Put(Bytes.add(Bytes.toBytes(CompanyId),
				Bytes.toBytes(DeviceId),
				Bytes.toBytes(DemandTime)));
		putDemand.add(Bytes.toBytes("P"), new byte[]{FpQualifier[23]}, 0,
				Bytes.toBytes(FPdemand));
		putDemand.add(Bytes.toBytes("P"), new byte[]{FpQualifier[24]}, 0,
				Bytes.toBytes(BPdemand));
		putDemand.add(Bytes.toBytes("P"), new byte[]{FpQualifier[25]}, 0,
				Bytes.toBytes(FQdemand));
		putDemand.add(Bytes.toBytes("P"), new byte[]{FpQualifier[26]}, 0,
				Bytes.toBytes(BQdemand));
		putDemand.add(Bytes.toBytes("P"), new byte[]{FpQualifier[27]}, 0,
				Bytes.toBytes(DemandTime));
		
        List<Put> puts = new ArrayList<Put>();
		puts.add(put);
		puts.add(putDemand);
		return puts;
	}
	
	// 辅助方法，添加colunmfamily U组的qualifier进put
	private void addFamilyU(Put put, Map<String, String[]> request, StringBuffer sb, Voltage voltage) {
		float Ua = NumberUtil.Float_valueOf(request, "Ua");
		float Ub = NumberUtil.Float_valueOf(request, "Ub");
		float Uc = NumberUtil.Float_valueOf(request, "Uc");
		voltage.ua = Ua;
		voltage.ub = Ub;
		voltage.uc = Uc;
		float U0 = NumberUtil.Float_valueOf(request, "U0");
		float Ua1 = NumberUtil.Float_valueOf(request, "Ua1");
		float Ub1 = NumberUtil.Float_valueOf(request, "Ub1");
		float Uc1 = NumberUtil.Float_valueOf(request, "Uc1");
		float JBa = NumberUtil.Float_valueOf(request, "JBa");
		float JBb = NumberUtil.Float_valueOf(request, "JBb");
		float JBc = NumberUtil.Float_valueOf(request, "JBc");
		float XBUav = NumberUtil.Float_valueOf(request, "XBUav");
		float XBUbv = NumberUtil.Float_valueOf(request, "XBUbv");
		float XBUcv = NumberUtil.Float_valueOf(request, "XBUcv");
		float NBPHu = NumberUtil.Float_valueOf(request, "NBPHu");
		float ZBPHu = NumberUtil.Float_valueOf(request, "ZBPHu");
		float JBoUp = NumberUtil.Float_valueOf(request, "JBoUp");
		float JBoUn = NumberUtil.Float_valueOf(request, "JBoUn");
        float value = 0f;
        long ts = 0l;

	/*	sb.append("Ua:"+Ua+"\n");
		sb.append("Ub:"+Ub+"\n");
		sb.append("Uc:"+Uc+"\n");
		sb.append("Ua1:"+Ua1+"\n");
		sb.append("Ub1:"+Ub1+"\n");
		sb.append("Uc1:"+Uc1+"\n");
		sb.append("NBPHu:"+NBPHu+"\n");
		sb.append("ZBPHu:"+ZBPHu+"\n");
		sb.append("JBoUp:"+JBoUp+"\n");
		sb.append("JBoUn:"+JBoUn+"\n");*/
		
		byte[] FuQualifier = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
				15, 16, 17 };
		put.add(Bytes.toBytes("U"), new byte[]{FuQualifier[0]}, 0,
				Bytes.toBytes(Ua));
		put.add(Bytes.toBytes("U"), new byte[]{FuQualifier[1]}, 0,
				Bytes.toBytes(Ub));
		put.add(Bytes.toBytes("U"), new byte[]{FuQualifier[2]}, 0,
				Bytes.toBytes(Uc));
        // 三相电压全小于0.5v将电压置为0
        if(Ua < 0.5 && Ub < 0.5 && Uc < 0.5) {
            value = 0f;
            ts = 4096l;
        } else {
            value = U0;
            ts = 0l;
        }
		put.add(Bytes.toBytes("U"), new byte[]{FuQualifier[3]}, ts,
				Bytes.toBytes(value));
		put.add(Bytes.toBytes("U"), new byte[]{FuQualifier[4]}, 0,
				Bytes.toBytes(Ua1));
		put.add(Bytes.toBytes("U"), new byte[]{FuQualifier[5]}, 0,
				Bytes.toBytes(Ub1));
		put.add(Bytes.toBytes("U"), new byte[]{FuQualifier[6]}, 0,
				Bytes.toBytes(Uc1));
        // 小于0.5v将电压置为0
		if(Ua < 0.5) {
		    value = 0f;
		    ts = 4096l;
		} else {
		    value = JBa;
		    ts = 0l;
		}
        put.add(Bytes.toBytes("U"), new byte[]{FuQualifier[7]}, ts,
                Bytes.toBytes(value));
        // 小于0.5v将电压置为0
        if(Ub < 0.5) {
            value = 0f;
            ts = 4096l;
        } else {
            value = JBb;
            ts = 0l;
        }
		put.add(Bytes.toBytes("U"), new byte[]{FuQualifier[8]}, ts,
				Bytes.toBytes(value));
        // 小于0.5v将电压置为0
        if(Uc < 0.5) {
            value = 0f;
            ts = 4096l;
        } else {
            value = JBc;
            ts = 0l;
        }
		put.add(Bytes.toBytes("U"), new byte[]{FuQualifier[9]}, ts,
				Bytes.toBytes(value));
		put.add(Bytes.toBytes("U"), new byte[]{FuQualifier[10]}, 0,
				Bytes.toBytes(XBUav));
		put.add(Bytes.toBytes("U"), new byte[]{FuQualifier[11]}, 0,
				Bytes.toBytes(XBUbv));
		put.add(Bytes.toBytes("U"), new byte[]{FuQualifier[12]}, 0,
				Bytes.toBytes(XBUcv));
        // 三相电压全小于1v将电压不平衡度置为0
        if(Ua < 0.5 && Ub < 0.5 && Uc < 0.5) {
            value = 0f;
            ts = 4096l;
        } else {
            value = NBPHu;
            ts = 0l;
        }
		put.add(Bytes.toBytes("U"), new byte[]{FuQualifier[13]}, ts,
				Bytes.toBytes(value));
        // 三相电压全小于1v将电压不平衡度置为0
        if(Ua < 0.5 && Ub < 0.5 && Uc < 0.5) {
            value = 0f;
            ts = 4096l;
        } else {
            value = ZBPHu;
            ts = 0l;
        }
		put.add(Bytes.toBytes("U"), new byte[]{FuQualifier[14]}, ts,
				Bytes.toBytes(value));
		put.add(Bytes.toBytes("U"), new byte[]{FuQualifier[15]}, 0,
				Bytes.toBytes(JBoUp));
        // 三相电压全小于0.5v将电压置为0
        if(Ua < 0.5 && Ub < 0.5 && Uc < 0.5) {
            value = 0f;
            ts = 4096l;
        } else {
            value = JBoUn;
            ts = 0l;
        }
		put.add(Bytes.toBytes("U"), new byte[]{FuQualifier[16]}, ts,
				Bytes.toBytes(value));
	}
	
	// 辅助方法，添加colunmfamily I组的qualifier进put
	private void addFamilyI(Put put, Map<String, String[]> request, StringBuffer sb, Current current) {
		
		float Ia = NumberUtil.Float_valueOf(request, "Ia");
		float Ib = NumberUtil.Float_valueOf(request, "Ib");
		float Ic = NumberUtil.Float_valueOf(request, "Ic");
		current.ia = Ia;
		current.ib = Ib;
		current.ic = Ic;
		float I0 = NumberUtil.Float_valueOf(request, "I0");
		float Ia1 = NumberUtil.Float_valueOf(request, "Ia1");
		float Ib1 = NumberUtil.Float_valueOf(request, "Ib1");
		float Ic1 = NumberUtil.Float_valueOf(request, "Ic1");
		float I01 = NumberUtil.Float_valueOf(request, "I01");
		float JBia = NumberUtil.Float_valueOf(request, "JBia");
		float JBib = NumberUtil.Float_valueOf(request, "JBib");
		float JBic = NumberUtil.Float_valueOf(request, "JBic");
		float XBIav = NumberUtil.Float_valueOf(request, "XBIav");
		float XBIbv = NumberUtil.Float_valueOf(request, "XBIbv");
		float XBIcv = NumberUtil.Float_valueOf(request, "XBIcv");
		float NBPHi = NumberUtil.Float_valueOf(request, "NBPHi");
		float ZBPHi = NumberUtil.Float_valueOf(request, "ZBPHi");
		float JBoIp = NumberUtil.Float_valueOf(request, "JBoIp");
		float JBoIn = NumberUtil.Float_valueOf(request, "JBoIn");
		float RI = NumberUtil.Float_valueOf(request, "ResidualCurrent");
		float value = 0f;
		long ts = 0l;
		
		/*sb.append("Ia:"+Ia+"\n");
		sb.append("Ib:"+Ib+"\n");
		sb.append("Ic:"+Ic+"\n");
		sb.append("Ia1:"+Ia1+"\n");
		sb.append("Ib1:"+Ib1+"\n");
		sb.append("Ic1:"+Ic1+"\n");
		sb.append("NBPHi:"+NBPHi+"\n");
		sb.append("ZBPHi:"+ZBPHi+"\n");
		sb.append("JBoIp:"+JBoIp+"\n");
		sb.append("JBoIn:"+JBoIn+"\n");
        if(RI!=-10000){
            sb.append("RI:"+RI+"\n");
        }*/
		
		byte[] FiQualifier = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
				15, 16, 17, 18 ,19};
		put.add(Bytes.toBytes("I"), new byte[]{FiQualifier[0]}, 0,
				Bytes.toBytes(Ia));
		put.add(Bytes.toBytes("I"), new byte[]{FiQualifier[1]}, 0,
				Bytes.toBytes(Ib));
		put.add(Bytes.toBytes("I"), new byte[]{FiQualifier[2]}, 0,
				Bytes.toBytes(Ic));
        // 小于0.05A将电流置为0
        if(Ia < 0.05 && Ib < 0.05 && Ic < 0.05) {
            value = 0f;
            ts = 4096l;
        } else {
            value = I0;
            ts = 0l;
        }
		put.add(Bytes.toBytes("I"), new byte[]{FiQualifier[3]}, ts,
				Bytes.toBytes(value));
		put.add(Bytes.toBytes("I"), new byte[]{FiQualifier[4]}, 0,
				Bytes.toBytes(Ia1));
		put.add(Bytes.toBytes("I"), new byte[]{FiQualifier[5]}, 0,
				Bytes.toBytes(Ib1));
		put.add(Bytes.toBytes("I"), new byte[]{FiQualifier[6]}, 0,
				Bytes.toBytes(Ic1));
        // 小于0.05A将电流置为0
        if(Ia < 0.05 && Ib < 0.05 && Ic < 0.05) {
            value = 0f;
            ts = 4096l;
        } else {
            value = I01;
            ts = 0l;
        }
		put.add(Bytes.toBytes("I"), new byte[]{FiQualifier[7]}, ts,
				Bytes.toBytes(value));
        // 小于0.05A将电流置为0
        if(Ia < 0.05) {
            value = 0f;
            ts = 4096l;
        } else {
            value = JBia;
            ts = 0l;
        }
		put.add(Bytes.toBytes("I"), new byte[]{FiQualifier[8]}, ts,
				Bytes.toBytes(value));
        // 小于0.05A将电流置为0
        if(Ib < 0.05) {
            value = 0f;
            ts = 4096l;
        } else {
            value = JBib;
            ts = 0l;
        }
		put.add(Bytes.toBytes("I"), new byte[]{FiQualifier[9]}, ts,
				Bytes.toBytes(value));
        // 小于0.05A将电流置为0
        if(Ic < 0.05) {
            value = 0f;
            ts = 4096l;
        } else {
            value = JBic;
            ts = 0l;
        }
		put.add(Bytes.toBytes("I"), new byte[]{FiQualifier[10]}, ts,
				Bytes.toBytes(value));
		put.add(Bytes.toBytes("I"), new byte[]{FiQualifier[11]}, 0,
				Bytes.toBytes(XBIav));
		put.add(Bytes.toBytes("I"), new byte[]{FiQualifier[12]}, 0,
				Bytes.toBytes(XBIbv));
		put.add(Bytes.toBytes("I"), new byte[]{FiQualifier[13]}, 0,
				Bytes.toBytes(XBIcv));
        // 小于0.05A将电流置为0
        if(Ia < 0.05 && Ib < 0.05 && Ic < 0.05) {
            value = 0f;
            ts = 4096l;
        } else {
            value = NBPHi;
            ts = 0l;
        }
		put.add(Bytes.toBytes("I"), new byte[]{FiQualifier[14]}, ts,
				Bytes.toBytes(value));
        // 小于0.05A将电流置为0
        if(Ia < 0.05 && Ib < 0.05 && Ic < 0.05) {
            value = 0f;
            ts = 4096l;
        } else {
            value = ZBPHi;
            ts = 0l;
        }
		put.add(Bytes.toBytes("I"), new byte[]{FiQualifier[15]}, ts,
				Bytes.toBytes(value));
		put.add(Bytes.toBytes("I"), new byte[]{FiQualifier[16]}, 0,
				Bytes.toBytes(JBoIp));
        // 小于0.05A将电流置为0
        if(Ia < 0.05 && Ib < 0.05 && Ic < 0.05) {
            value = 0f;
            ts = 4096l;
        } else {
            value = JBoIn;
            ts = 0l;
        }
		put.add(Bytes.toBytes("I"), new byte[]{FiQualifier[17]}, ts,
				Bytes.toBytes(value));
	    if(RI >= 0){
	        put.add(Bytes.toBytes("I"), new byte[]{FiQualifier[18]}, 0,
	                Bytes.toBytes(RI));
	    }

	}
	
	// 辅助方法，添加colunmfamily P组的qualifier进put
	private void addFamilyP(Put put, Map<String, String[]> request, StringBuffer sb, Voltage voltage, Current current) {
		
		float Pa = NumberUtil.Float_valueOf(request, "Pa");
		float Pb = NumberUtil.Float_valueOf(request, "Pb");
		float Pc = NumberUtil.Float_valueOf(request, "Pc");
		float P0 = NumberUtil.Float_valueOf(request, "P0");
		float Pa1 = NumberUtil.Float_valueOf(request, "Pa1");
		float Pb1 = NumberUtil.Float_valueOf(request, "Pb1");
		float Pc1 = NumberUtil.Float_valueOf(request, "Pc1");
		float P01 = NumberUtil.Float_valueOf(request, "P01");
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
		float Q01 = NumberUtil.Float_valueOf(request, "Q01");
		float PRF = NumberUtil.Float_valueOf(request, "PRF");
		float PFT = NumberUtil.Float_valueOf(request, "PFT");
		float FHL = NumberUtil.Float_valueOf(request, "FHL");
		
/*		sb.append("Pa:"+Pa+"\n");
		sb.append("Pb:"+Pb+"\n");
		sb.append("Pc:"+Pc+"\n");
		sb.append("P0:"+P0+"\n");
		sb.append("Sa:"+Sa+"\n");
		sb.append("Sb:"+Sb+"\n");
		sb.append("Sc:"+Sc+"\n");
		sb.append("S0:"+S0+"\n");
		sb.append("Qa:"+Qa+"\n");
		sb.append("Qb:"+Qb+"\n");
		sb.append("Qc:"+Qc+"\n");
		sb.append("Q0:"+Q0+"\n");
		sb.append("Ca:"+Ca+"\n");
		sb.append("Cb:"+Cb+"\n");
		sb.append("Cc:"+Cc+"\n");
		sb.append("PFT:"+PFT+"\n")*/;
		
		byte[] FpQualifier = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
				15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28 };
		put.add(Bytes.toBytes("P"), new byte[]{FpQualifier[0]}, 0,
				Bytes.toBytes(Pa));
		put.add(Bytes.toBytes("P"), new byte[]{FpQualifier[1]}, 0,
				Bytes.toBytes(Pb));
		put.add(Bytes.toBytes("P"), new byte[]{FpQualifier[2]}, 0,
				Bytes.toBytes(Pc));
		put.add(Bytes.toBytes("P"), new byte[]{FpQualifier[3]}, 0,
				Bytes.toBytes(P0));
		put.add(Bytes.toBytes("P"), new byte[]{FpQualifier[4]}, 0,
				Bytes.toBytes(Pa1));
		put.add(Bytes.toBytes("P"), new byte[]{FpQualifier[5]}, 0,
				Bytes.toBytes(Pb1));
		put.add(Bytes.toBytes("P"), new byte[]{FpQualifier[6]}, 0,
				Bytes.toBytes(Pc1));
		put.add(Bytes.toBytes("P"), new byte[]{FpQualifier[7]}, 0,
				Bytes.toBytes(P01));
		put.add(Bytes.toBytes("P"), new byte[]{FpQualifier[8]}, 0,
				Bytes.toBytes(Qa));
		put.add(Bytes.toBytes("P"), new byte[]{FpQualifier[9]}, 0,
				Bytes.toBytes(Qb));
		put.add(Bytes.toBytes("P"), new byte[]{FpQualifier[10]}, 0,
				Bytes.toBytes(Qc));
		put.add(Bytes.toBytes("P"), new byte[]{FpQualifier[11]}, 0,
				Bytes.toBytes(Q0));
		put.add(Bytes.toBytes("P"), new byte[]{FpQualifier[12]}, 0,
				Bytes.toBytes(Sa));
		put.add(Bytes.toBytes("P"), new byte[]{FpQualifier[13]}, 0,
				Bytes.toBytes(Sb));
		put.add(Bytes.toBytes("P"), new byte[]{FpQualifier[14]}, 0,
				Bytes.toBytes(Sc));
		put.add(Bytes.toBytes("P"), new byte[]{FpQualifier[15]}, 0,
				Bytes.toBytes(S0));
		float value = 0f;
		long ts = 0l;
		// 电压小于0.5v或电流小于0.05A将功率因数置为0
		if(voltage.ua < 0.5 || current.ia < 0.05) {
		    value = 0f;
		    ts = 4096l;
		} else {
		    value = Ca;
            ts = 0;
		}
		put.add(Bytes.toBytes("P"), new byte[]{FpQualifier[16]}, ts,
				Bytes.toBytes(value));
        // 电压小于0.5v或电流小于0.05A将功率因数置为0
        if(voltage.ub < 0.5 || current.ib < 0.05) {
            value = 0f;
            ts = 4096l;
        } else {
            value = Cb;
            ts = 0;
        }
		put.add(Bytes.toBytes("P"), new byte[]{FpQualifier[17]}, ts,
				Bytes.toBytes(value));
        // 电压小于0.5v或电流小于0.05A将功率因数置为0
        if(voltage.uc < 0.5 || current.ic < 0.05) {
            value = 0f;
            ts = 4096l;
        } else {
            value = Cc;
            ts = 0;
        }
		put.add(Bytes.toBytes("P"), new byte[]{FpQualifier[18]}, ts,
				Bytes.toBytes(value));
		put.add(Bytes.toBytes("P"), new byte[]{FpQualifier[19]}, 0,
				Bytes.toBytes(Q01));
        // 电压小于0.5v或电流小于0.05A将功率因数置为0
        if((voltage.ua < 0.5 && voltage.ub < 0.5 && voltage.uc < 0.5) || 
                (current.ia < 0.05 && current.ib < 0.05 && current.ic < 0.05)) {
            value = 0f;
            ts = 4096l;
        } else {
            value = PRF;
            ts = 0;
        }
		put.add(Bytes.toBytes("P"), new byte[]{FpQualifier[20]}, ts,
				Bytes.toBytes(value));
        // 电压小于0.5v或电流小于0.05A将功率因数置为0
        if((voltage.ua < 0.5 && voltage.ub < 0.5 && voltage.uc < 0.5) || 
                (current.ia < 0.05 && current.ib < 0.05 && current.ic < 0.05)) {
            value = 0f;
            ts = 4096l;
        } else {
            value = PFT;
            ts = 0;
        }
		put.add(Bytes.toBytes("P"), new byte[]{FpQualifier[21]}, ts,
				Bytes.toBytes(value));
		put.add(Bytes.toBytes("P"), new byte[]{FpQualifier[22]}, 0,
				Bytes.toBytes(FHL));
	}
	
	// 辅助方法，添加colunmfamily D组的qualifier进put
	private void addFamilyD(Put put, Map<String, String[]> request, StringBuffer sb) {
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
		float F = NumberUtil.Float_valueOf(request, "F");
		double PeakEpi = NumberUtil.Double_valueOf(request, "PeakEpi");
		double FlatEpi = NumberUtil.Double_valueOf(request, "FlatEpi");
		double ValleyEpi = NumberUtil.Double_valueOf(request, "ValleyEpi");
		float UaLongFlicker = NumberUtil.Float_valueOf(request
				, "UaLongFlicker");
		float UbLongFlicker = NumberUtil.Float_valueOf(request
				, "UbLongFlicker");
		float UcLongFlicker = NumberUtil.Float_valueOf(request
				, "UcLongFlicker");
		float UaShortFlicker = NumberUtil.Float_valueOf(request
				, "UaShortFlicker");
		float UbShortFlicker = NumberUtil.Float_valueOf(request
				, "UbShortFlicker");
		float UcShortFlicker = NumberUtil.Float_valueOf(request
				, "UcShortFlicker");
		
		boolean flickerToStore = true;
		try {
			short DeviceId = NumberUtil.Short_valueOf(request, "DeviceId");
			// TODO 对闪变值做特殊处理，没有考虑到闪变值为极小值的情况，这里不能使用DataFormatUtil
			String flicker = UaLongFlicker + UcShortFlicker + "";
			synchronized (deviceIDAndFlickerMaps) {
				String value = deviceIDAndFlickerMaps.get(DeviceId);
				if(value != null && value.equals(flicker)) {
					flickerToStore = false;
				} else {
					deviceIDAndFlickerMaps.put(DeviceId, flicker);
				}
			}
		} catch (Exception e) {}
		
		sb.append("Epi:"+Epi+"\n");
	    sb.append("PeakEpi:"+PeakEpi+"\n");
	    sb.append("FlatEpi:"+FlatEpi+"\n");
	    sb.append("ValleyEpi:"+ValleyEpi+"\n");
		
		byte[] FdQualifier = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
				15, 16, 17, 18, 19, 20, 21};
		put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[0]}, 0,
				Bytes.toBytes(Epi));
		put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[1]}, 0,
				Bytes.toBytes(Epia));
		put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[2]}, 0,
				Bytes.toBytes(Epib));
		put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[3]}, 0,
				Bytes.toBytes(Epic));
		put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[4]}, 0,
				Bytes.toBytes(Epoa));
		put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[5]}, 0,
				Bytes.toBytes(Epob));
		put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[6]}, 0,
				Bytes.toBytes(Epoc));
		put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[7]}, 0,
				Bytes.toBytes(Epo));
		put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[8]}, 0,
				Bytes.toBytes(EQind));
		put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[9]}, 0,
				Bytes.toBytes(EQcap));
		put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[10]}, 0l,
				Bytes.toBytes(F));
		put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[11]}, 0,
				Bytes.toBytes(PeakEpi));
		put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[12]}, 0,
				Bytes.toBytes(FlatEpi));
		put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[13]}, 0,
				Bytes.toBytes(ValleyEpi));
		if(flickerToStore) {
			put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[14]}, 0,
					Bytes.toBytes(UaShortFlicker));
			put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[15]}, 0,
					Bytes.toBytes(UbShortFlicker));
			put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[16]}, 0,
					Bytes.toBytes(UcShortFlicker));
			put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[17]}, 0,
					Bytes.toBytes(UaLongFlicker));
			put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[18]}, 0,
					Bytes.toBytes(UbLongFlicker));
			put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[19]}, 0,
					Bytes.toBytes(UcLongFlicker));
		}
		/*put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[20]}, 0,
				Bytes.toBytes(CumulativeTime));*/

	}
	
	// 辅助方法，传入列族名，列族中qualifier的值，谐波可25次，50次，通过判断数组的大小，
	// 得出qualifier标识符的数目，用在A，B，C电流谐波，ABC相电压谐波，分别对应列族，{R,S,T,X,Y,Z}
	private void addFamilyXB(Put put, Map<String, String[]> request, String familyName, String requestParameter, float vv) {
	    try
        {
	        float[] value = DataTransferUtil.transferStrArrTofloatArr(request
	                .get(requestParameter));
	        byte qualifier;

            if(familyName == "X" || familyName == "Y" || familyName == "Z") {
                // 小于0.5v将电压置为0
                if(vv < 0.5) {
                    for (int i = 0; i < 24; i++) { // 2-25次谐波
                        qualifier = (byte) (i + 2);
                        put.add(Bytes.toBytes(familyName), new byte[]{qualifier}, 4096l,
                                Bytes.toBytes(0f));
                    }
                } else {
                    for (int i = 0; i < 24; i++) { // 2-25次谐波
                        qualifier = (byte) (i + 2);
                        put.add(Bytes.toBytes(familyName), new byte[]{qualifier}, 0,
                                Bytes.toBytes(value[i]));
                    }
                }
            } else {
                // 小于0.05A将电流置为0
                if(vv < 0.05) {
                    for (int i = 0; i < 24; i++) { // 2-25次谐波
                        qualifier = (byte) (i + 2);
                        put.add(Bytes.toBytes(familyName), new byte[]{qualifier}, 4096l,
                                Bytes.toBytes(0f));
                    }
                } else {
                    for (int i = 0; i < 24; i++) { // 2-25次谐波
                        qualifier = (byte) (i + 2);
                        put.add(Bytes.toBytes(familyName), new byte[]{qualifier}, 0,
                                Bytes.toBytes(value[i]));
                    }
                }
            }
        } catch (Exception e)
        {
            e.printStackTrace();
        }
	}

	@Override
	public boolean put(final List<Put> put){		
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
			String path = folder + File.separator + "backup2" + File.separator + "monitordata" + File.separator 
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
