package com.nikey.hbase;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
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
import com.nikey.util.DateUtil;
import com.nikey.util.JsonUtil;
import com.nikey.util.NumberUtil;
import com.nikey.util.PropUtil;
import com.nikey.util.ServiceHelper;

/**
 * @author jayzee
 * @date 28 Sep, 2014 
 * GrouppTable
 */
public class CommerrHTableMapper implements HTableMapper {

    public static void main(String[] args) {
        CommerrHTableMapper mapper = new CommerrHTableMapper();
        Map<String, String[]> request = new HashMap<String, String[]>();
        request.put("EventType", new String[] { "1" });
        request.put("CompanyId", new String[] { "7" });
        request.put("DeviceId", new String[] { "7006" });
        request.put("InsertTime", new String[] { "1487570400" });
        request.put("EndTime", new String[] { "1487570400" });
        List<Put> put = mapper.convertParameterMapToPut(request);
        mapper.put(put);
        System.exit(0);
    }

    /**
     * slfj
     */
    Logger logger = LoggerFactory.getLogger(getClass());

    public Put getPutForMonitordataFix(short companyId, short DeviceId,
            long time) {
        Put put = new Put(Bytes.add(Bytes.toBytes(companyId),
                Bytes.toBytes(DeviceId), Bytes.toBytes(time)));

        addFamilyU(put);// monitortable columnFamily U
        addFamilyI(put);// monitortable columnFamily I
        addFamilyP(put);// monitortable columnFamily P
        addFamilyD(put);// monitortable columnFamily D

        addFamilyXB(put, "R", "XBia");// monitortable columnFamily R S T X Y Z
        addFamilyXB(put, "S", "XBib");
        addFamilyXB(put, "T", "XBic");
        addFamilyXB(put, "X", "XBua");
        addFamilyXB(put, "Y", "XBub");
        addFamilyXB(put, "Z", "XBuc");

        return put;
    }

    public Put getPutForTemperatureFix(short companyId, short DeviceId,
            long time, int cases) {
        Put put = new Put(Bytes.add(Bytes.toBytes(companyId),
                Bytes.toBytes(DeviceId), Bytes.toBytes(time)));

        byte[] Qualifier = { 1, 2, 3, 4, 5 };
        long timestamp = 4096l;

        switch (cases) {
        case 1:
            put.add(Bytes.toBytes("A"), new byte[] { Qualifier[0] }, timestamp,
                    Bytes.toBytes(0f));
            put.add(Bytes.toBytes("A"), new byte[] { Qualifier[1] }, timestamp,
                    Bytes.toBytes(0f));
            put.add(Bytes.toBytes("A"), new byte[] { Qualifier[2] }, timestamp,
                    Bytes.toBytes(0f));
            break;
        case 4:
            put.add(Bytes.toBytes("A"), new byte[] { Qualifier[3] }, timestamp,
                    Bytes.toBytes(0f));
            break;
        case 5:
            put.add(Bytes.toBytes("A"), new byte[] { Qualifier[4] }, timestamp,
                    Bytes.toBytes(0f));
            break;
        }

        return put;
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

        byte EventType = Byte.valueOf(request.get("EventType")[0]);
        long EndTime = Long.valueOf(request.get("EndTime")[0]);
        EndTime = EndTime * 1000l; // 放大为毫秒值

        Abnormaldata abnormaldata = new Abnormaldata();
        abnormaldata.setDeviceId(DeviceId);
        abnormaldata.setStartTime(DateUtil.formatToHHMMSS(InsertTime));
        abnormaldata.setEventType(EventType);
        abnormaldata.setEndTime(DateUtil.formatToHHMMSS(EndTime));
        ServiceHelper.instance().getAbnormalDataService()
                .handleCommerr(abnormaldata, EndTime);

        // CEIU初次建立SOCKET连接时，会判断是否距离上一次周期数据时间超过5分钟，是则设置EndTime等于InsertTime
        // 或是数据断开超过15分钟（900秒）
        if (EndTime == InsertTime || Math.abs(InsertTime - EndTime) > 900000) {
            // do nothing
        } else {
            return null;
        }

        Put put = null;
        long timestamp = 4096l;
        int type = 0;

        switch (EventType) {
        case 1:
            put = new Put(Bytes.add(Bytes.toBytes(CompanyId),
                    Bytes.toBytes(DeviceId), Bytes.toBytes(InsertTime)));

            addFamilyU(put);// monitortable columnFamily U
            addFamilyI(put);// monitortable columnFamily I
            addFamilyP(put);// monitortable columnFamily P
            addFamilyD(put);// monitortable columnFamily D

            addFamilyXB(put, "R", "XBia");// monitortable columnFamily R S T X Y
                                          // Z
            addFamilyXB(put, "S", "XBib");
            addFamilyXB(put, "T", "XBic");
            addFamilyXB(put, "X", "XBua");
            addFamilyXB(put, "Y", "XBub");
            addFamilyXB(put, "Z", "XBuc");

            put.setId("1");
            break;
        case 5: // temhum485
            put = new Put(Bytes.add(Bytes.toBytes(CompanyId),
                    Bytes.toBytes(DeviceId), Bytes.toBytes(InsertTime)));
            type = ServiceHelper.instance().getKlm4134Service()
                    .getRelationMap(DeviceId);
            if (type != 0) {
                if (type == 1 || type == 2) {
                    put.add(Bytes.toBytes("A"),
                            new byte[] { Byte.valueOf("4") }, timestamp,
                            Bytes.toBytes(0f));
                } else {
                    put.add(Bytes.toBytes("A"),
                            new byte[] { Byte.valueOf("5") }, timestamp,
                            Bytes.toBytes(0f));
                }
            }
            put.add(Bytes.toBytes("A"), new byte[] { Byte.valueOf("6") },
                    timestamp, Bytes.toBytes(0f));
            put.setId("5");
            break;
        case 6: // PT100 电机温度监测
        	put = new Put(Bytes.add(Bytes.toBytes(CompanyId),
                    Bytes.toBytes(DeviceId), Bytes.toBytes(InsertTime)));
        	addFamilyA(put);
        	put.setId("6");
        	break;
        default:
            break;
        }

        List<Put> puts = new ArrayList<Put>();
        if (put != null) puts.add(put);
        return puts;
    }
    
    //辅助方法，添加columnfamily A组的qualifier进put
    private void addFamilyA(Put put) {
		long InsertTime = 4096l;
		float head_da = 0f;
		float tail_da = 0f;
		float head_db = 0f;
		float tail_db = 0f;
		float head_dc = 0f;
		float tail_dc = 0f;
		float front_motor = 0f;
		float back_motor = 0f;
		float front_pump = 0f;
		float back_pump = 0f;
		
		long timestamp = 4096l;
		
		byte[] FuQualifier = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };
		
		put.add(Bytes.toBytes("A"), new byte[] { FuQualifier[0] }, timestamp,
                Bytes.toBytes(InsertTime));
		put.add(Bytes.toBytes("A"), new byte[] { FuQualifier[1] }, timestamp,
                Bytes.toBytes(head_da));
		put.add(Bytes.toBytes("A"), new byte[] { FuQualifier[2] }, timestamp,
                Bytes.toBytes(tail_da));
		put.add(Bytes.toBytes("A"), new byte[] { FuQualifier[3] }, timestamp,
                Bytes.toBytes(head_db));
		put.add(Bytes.toBytes("A"), new byte[] { FuQualifier[4] }, timestamp,
                Bytes.toBytes(tail_db));
		put.add(Bytes.toBytes("A"), new byte[] { FuQualifier[5] }, timestamp,
                Bytes.toBytes(head_dc));
		put.add(Bytes.toBytes("A"), new byte[] { FuQualifier[6] }, timestamp,
                Bytes.toBytes(tail_dc));
		put.add(Bytes.toBytes("A"), new byte[] { FuQualifier[7] }, timestamp,
                Bytes.toBytes(front_motor));
		put.add(Bytes.toBytes("A"), new byte[] { FuQualifier[8] }, timestamp,
                Bytes.toBytes(back_motor));
		put.add(Bytes.toBytes("A"), new byte[] { FuQualifier[9] }, timestamp,
                Bytes.toBytes(front_pump));
		put.add(Bytes.toBytes("A"), new byte[] { FuQualifier[10] }, timestamp,
                Bytes.toBytes(back_pump));
	}

    // 辅助方法，添加colunmfamily U组的qualifier进put
    private void addFamilyU(Put put) {

        float Ua = 0;
        float Ub = 0;
        float Uc = 0;
        float U0 = 0;
        float Ua1 = 0;
        float Ub1 = 0;
        float Uc1 = 0;
        float JBa = 0;
        float JBb = 0;
        float JBc = 0;
        float XBUav = 0;
        float XBUbv = 0;
        float XBUcv = 0;
        float NBPHu = 0;
        float ZBPHu = 0;
        float JBoUp = 0;
        float JBoUn = 0;

        long timestamp = 4096;

        byte[] FuQualifier = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
                15, 16, 17 };
        put.add(Bytes.toBytes("U"), new byte[] { FuQualifier[0] }, timestamp,
                Bytes.toBytes(Ua));
        put.add(Bytes.toBytes("U"), new byte[] { FuQualifier[1] }, timestamp,
                Bytes.toBytes(Ub));
        put.add(Bytes.toBytes("U"), new byte[] { FuQualifier[2] }, timestamp,
                Bytes.toBytes(Uc));
        put.add(Bytes.toBytes("U"), new byte[] { FuQualifier[3] }, timestamp,
                Bytes.toBytes(U0));
        put.add(Bytes.toBytes("U"), new byte[] { FuQualifier[4] }, timestamp,
                Bytes.toBytes(Ua1));
        put.add(Bytes.toBytes("U"), new byte[] { FuQualifier[5] }, timestamp,
                Bytes.toBytes(Ub1));
        put.add(Bytes.toBytes("U"), new byte[] { FuQualifier[6] }, timestamp,
                Bytes.toBytes(Uc1));
        put.add(Bytes.toBytes("U"), new byte[] { FuQualifier[7] }, timestamp,
                Bytes.toBytes(JBa));
        put.add(Bytes.toBytes("U"), new byte[] { FuQualifier[8] }, timestamp,
                Bytes.toBytes(JBb));
        put.add(Bytes.toBytes("U"), new byte[] { FuQualifier[9] }, timestamp,
                Bytes.toBytes(JBc));
        put.add(Bytes.toBytes("U"), new byte[] { FuQualifier[10] }, timestamp,
                Bytes.toBytes(XBUav));
        put.add(Bytes.toBytes("U"), new byte[] { FuQualifier[11] }, timestamp,
                Bytes.toBytes(XBUbv));
        put.add(Bytes.toBytes("U"), new byte[] { FuQualifier[12] }, timestamp,
                Bytes.toBytes(XBUcv));
        put.add(Bytes.toBytes("U"), new byte[] { FuQualifier[13] }, timestamp,
                Bytes.toBytes(NBPHu));
        put.add(Bytes.toBytes("U"), new byte[] { FuQualifier[14] }, timestamp,
                Bytes.toBytes(ZBPHu));
        put.add(Bytes.toBytes("U"), new byte[] { FuQualifier[15] }, timestamp,
                Bytes.toBytes(JBoUp));
        put.add(Bytes.toBytes("U"), new byte[] { FuQualifier[16] }, timestamp,
                Bytes.toBytes(JBoUn));
    }

    // 辅助方法，添加colunmfamily I组的qualifier进put
    private void addFamilyI(Put put) {

        float Ia = 0;
        float Ib = 0;
        float Ic = 0;
        float I0 = 0;
        float Ia1 = 0;
        float Ib1 = 0;
        float Ic1 = 0;
        float I01 = 0;
        float JBia = 0;
        float JBib = 0;
        float JBic = 0;
        float XBIav = 0;
        float XBIbv = 0;
        float XBIcv = 0;
        float NBPHi = 0;
        float ZBPHi = 0;
        float JBoIp = 0;
        float JBoIn = 0;

        long timestamp = 4096;

        byte[] FiQualifier = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
                15, 16, 17, 18 };
        put.add(Bytes.toBytes("I"), new byte[] { FiQualifier[0] }, timestamp,
                Bytes.toBytes(Ia));
        put.add(Bytes.toBytes("I"), new byte[] { FiQualifier[1] }, timestamp,
                Bytes.toBytes(Ib));
        put.add(Bytes.toBytes("I"), new byte[] { FiQualifier[2] }, timestamp,
                Bytes.toBytes(Ic));
        put.add(Bytes.toBytes("I"), new byte[] { FiQualifier[3] }, timestamp,
                Bytes.toBytes(I0));
        put.add(Bytes.toBytes("I"), new byte[] { FiQualifier[4] }, timestamp,
                Bytes.toBytes(Ia1));
        put.add(Bytes.toBytes("I"), new byte[] { FiQualifier[5] }, timestamp,
                Bytes.toBytes(Ib1));
        put.add(Bytes.toBytes("I"), new byte[] { FiQualifier[6] }, timestamp,
                Bytes.toBytes(Ic1));
        put.add(Bytes.toBytes("I"), new byte[] { FiQualifier[7] }, timestamp,
                Bytes.toBytes(I01));
        put.add(Bytes.toBytes("I"), new byte[] { FiQualifier[8] }, timestamp,
                Bytes.toBytes(JBia));
        put.add(Bytes.toBytes("I"), new byte[] { FiQualifier[9] }, timestamp,
                Bytes.toBytes(JBib));
        put.add(Bytes.toBytes("I"), new byte[] { FiQualifier[10] }, timestamp,
                Bytes.toBytes(JBic));
        put.add(Bytes.toBytes("I"), new byte[] { FiQualifier[11] }, timestamp,
                Bytes.toBytes(XBIav));
        put.add(Bytes.toBytes("I"), new byte[] { FiQualifier[12] }, timestamp,
                Bytes.toBytes(XBIbv));
        put.add(Bytes.toBytes("I"), new byte[] { FiQualifier[13] }, timestamp,
                Bytes.toBytes(XBIcv));
        put.add(Bytes.toBytes("I"), new byte[] { FiQualifier[14] }, timestamp,
                Bytes.toBytes(NBPHi));
        put.add(Bytes.toBytes("I"), new byte[] { FiQualifier[15] }, timestamp,
                Bytes.toBytes(ZBPHi));
        put.add(Bytes.toBytes("I"), new byte[] { FiQualifier[16] }, timestamp,
                Bytes.toBytes(JBoIp));
        put.add(Bytes.toBytes("I"), new byte[] { FiQualifier[17] }, timestamp,
                Bytes.toBytes(JBoIn));

    }

    // 辅助方法，添加colunmfamily P组的qualifier进put
    private void addFamilyP(Put put) {

        float Pa = 0;
        float Pb = 0;
        float Pc = 0;
        float P0 = 0;
        float Pa1 = 0;
        float Pb1 = 0;
        float Pc1 = 0;
        float P01 = 0;
        float Qa = 0;
        float Qb = 0;
        float Qc = 0;
        float Q0 = 0;
        float Sa = 0;
        float Sb = 0;
        float Sc = 0;
        float S0 = 0;
        float Ca = 0;
        float Cb = 0;
        float Cc = 0;
        float Q01 = 0;
        float PRF = 0;
        float PFT = 0;
        float FHL = 0;
        float FPdemand = 0;
        float BPdemand = 0;
        float FQdemand = 0;
        float BQdemand = 0;

        long timestamp = 4096;

        byte[] FpQualifier = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
                15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27 };
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[0] }, timestamp,
                Bytes.toBytes(Pa));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[1] }, timestamp,
                Bytes.toBytes(Pb));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[2] }, timestamp,
                Bytes.toBytes(Pc));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[3] }, timestamp,
                Bytes.toBytes(P0));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[4] }, timestamp,
                Bytes.toBytes(Pa1));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[5] }, timestamp,
                Bytes.toBytes(Pb1));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[6] }, timestamp,
                Bytes.toBytes(Pc1));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[7] }, timestamp,
                Bytes.toBytes(P01));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[8] }, timestamp,
                Bytes.toBytes(Qa));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[9] }, timestamp,
                Bytes.toBytes(Qb));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[10] }, timestamp,
                Bytes.toBytes(Qc));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[11] }, timestamp,
                Bytes.toBytes(Q0));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[12] }, timestamp,
                Bytes.toBytes(Sa));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[13] }, timestamp,
                Bytes.toBytes(Sb));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[14] }, timestamp,
                Bytes.toBytes(Sc));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[15] }, timestamp,
                Bytes.toBytes(S0));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[16] }, timestamp,
                Bytes.toBytes(Ca));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[17] }, timestamp,
                Bytes.toBytes(Cb));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[18] }, timestamp,
                Bytes.toBytes(Cc));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[19] }, timestamp,
                Bytes.toBytes(Q01));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[20] }, timestamp,
                Bytes.toBytes(PRF));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[21] }, timestamp,
                Bytes.toBytes(PFT));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[22] }, timestamp,
                Bytes.toBytes(FHL));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[23] }, timestamp,
                Bytes.toBytes(FPdemand));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[24] }, timestamp,
                Bytes.toBytes(BPdemand));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[25] }, timestamp,
                Bytes.toBytes(FQdemand));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[26] }, timestamp,
                Bytes.toBytes(BQdemand));
    }

    // 辅助方法，添加colunmfamily D组的qualifier进put
    private void addFamilyD(Put put) {

        /*
         * double Epi = 0; double Epia = 0; double Epib = 0; double Epic = 0;
         * double Epoa = 0; double Epob = 0; double Epoc = 0; double Epo = 0;
         * double EQind = 0; double EQcap = 0;
         */
        float F = 0;
        /*
         * double PeakEpi = 0; double FlatEpi = 0; double ValleyEpi = 0;
         */
        float UaLongFlicker = 0;
        float UbLongFlicker = 0;
        float UcLongFlicker = 0;
        float UaShortFlicker = 0;
        float UbShortFlicker = 0;
        float UcShortFlicker = 0;
        // long CumulativeTime = 0;

        long timestamp = 4096;

        byte[] FdQualifier = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
                15, 16, 17, 18, 19, 20, 21 };
        /*
         * put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[0]}, 0,
         * Bytes.toBytes(Epi)); put.add(Bytes.toBytes("D"), new
         * byte[]{FdQualifier[1]}, 0, Bytes.toBytes(Epia));
         * put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[2]}, 0,
         * Bytes.toBytes(Epib)); put.add(Bytes.toBytes("D"), new
         * byte[]{FdQualifier[3]}, 0, Bytes.toBytes(Epic));
         * put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[4]}, 0,
         * Bytes.toBytes(Epoa)); put.add(Bytes.toBytes("D"), new
         * byte[]{FdQualifier[5]}, 0, Bytes.toBytes(Epob));
         * put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[6]}, 0,
         * Bytes.toBytes(Epoc)); put.add(Bytes.toBytes("D"), new
         * byte[]{FdQualifier[7]}, 0, Bytes.toBytes(Epo));
         * put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[8]}, 0,
         * Bytes.toBytes(EQind)); put.add(Bytes.toBytes("D"), new
         * byte[]{FdQualifier[9]}, 0, Bytes.toBytes(EQcap));
         */
        put.add(Bytes.toBytes("D"), new byte[] { FdQualifier[10] }, timestamp,
                Bytes.toBytes(F));
        /*
         * put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[11]}, 0,
         * Bytes.toBytes(PeakEpi)); put.add(Bytes.toBytes("D"), new
         * byte[]{FdQualifier[12]}, 0, Bytes.toBytes(FlatEpi));
         * put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[13]}, 0,
         * Bytes.toBytes(ValleyEpi));
         */
        put.add(Bytes.toBytes("D"), new byte[] { FdQualifier[14] }, timestamp,
                Bytes.toBytes(UaShortFlicker));
        put.add(Bytes.toBytes("D"), new byte[] { FdQualifier[15] }, timestamp,
                Bytes.toBytes(UbShortFlicker));
        put.add(Bytes.toBytes("D"), new byte[] { FdQualifier[16] }, timestamp,
                Bytes.toBytes(UcShortFlicker));
        put.add(Bytes.toBytes("D"), new byte[] { FdQualifier[17] }, timestamp,
                Bytes.toBytes(UaLongFlicker));
        put.add(Bytes.toBytes("D"), new byte[] { FdQualifier[18] }, timestamp,
                Bytes.toBytes(UbLongFlicker));
        put.add(Bytes.toBytes("D"), new byte[] { FdQualifier[19] }, timestamp,
                Bytes.toBytes(UcLongFlicker));
        /*
         * put.add(Bytes.toBytes("D"), new byte[]{FdQualifier[20]}, 0,
         * Bytes.toBytes(CumulativeTime));
         */

    }

    // 辅助方法，传入列族名，列族中qualifier的值，谐波可25次，50次，通过判断数组的大小，
    // 得出qualifier标识符的数目，用在A，B，C电流谐波，ABC相电压谐波，分别对应列族，{R,S,T,X,Y,Z}
    private void addFamilyXB(Put put, String familyName, String requestParameter) {
        float value = 0f;
        byte qualifier;
        long timestamp = 4096;
        for (int i = 0; i < 24; i++) { // 2-25次谐波
            qualifier = (byte) (i + 2);
            put.add(Bytes.toBytes(familyName), new byte[] { qualifier },
                    timestamp, Bytes.toBytes(value));
        }
    }

    @Override
    public boolean put(final List<Put> puts) {

        boolean flag = true;
        HTableInterface htable = null;

        try {
            Put put = puts.get(0);
            String id = put.getId();
            switch (id) {
            case "1":
                htable = HbaseTablePool.instance().getHtable(
                        PropUtil.getString("hbase_monitordata_name")); // NEMU
                break;
            case "5":
                htable = HbaseTablePool.instance().getHtable(
                        PropUtil.getString("hbase_temperature_name")); // 温度表
                break;
            case "6":
            	htable = HbaseTablePool.instance().getHtable(
                        PropUtil.getString("hbase_motor_temp")); // 电机温度表
            	break;
            default:
                flag = false;
                break;
            }

            if (htable != null) {
                // final long start = System.currentTimeMillis();
                htable.put(put);
                htable.flushCommits();// write to hbase immediately
                htable.close();
                // logger.info("put to hbase success..." +
                // (System.currentTimeMillis() - start));
            } else {
                flag = false;
            }
        } catch (Exception e) {
            flag = false;
            e.printStackTrace();
        }

        return flag;
    }

    @Override
    public ResultScanner getResultScannerWithParameterMap(Scan scan) {
        return null;
    }

    /**
     * backup the terminal settings to local file
     * 
     * @param deviceid
     * @param backupstr
     */
    @SuppressWarnings("unused")
    private void terminalSettingsBackup(Short deviceid, String backupstr,
            int type) {
        File directory = new File("");// 设定为当前文件夹
        try {
            String folder = directory.getAbsolutePath();
            String path = folder + File.separator + "backup" + File.separator
                    + "commerr" + File.separator + type + File.separator
                    + deviceid + ".txt";

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

}