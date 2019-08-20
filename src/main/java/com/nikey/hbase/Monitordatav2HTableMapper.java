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

/**
 * @author jayzee
 * @date 25 Sep, 2014 Monitordata HTable Mapper instance
 */
public class Monitordatav2HTableMapper implements HTableMapper {

    /**
     * slfj
     */
    Logger logger = LoggerFactory.getLogger(getClass());

    public static void main(String[] args) {
        System.out.println(1511849080l - (1511849080l % 3600));
        System.out.println(1511848800l % 3600);
    }

    private ThreadLocal<HTableInterface> htables = new ThreadLocal<HTableInterface>();
    private static ConcurrentHashMap<String,Long>  deviceIdTimestamp = new ConcurrentHashMap<>();//用于记录每个deviceid上一次缓存时间戳

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
        int DeviceId = 0;
        long InsertTime = 0l;
        try {
            CompanyId = NumberUtil.Short_valueOf(request, "CompanyId");
            DeviceId = NumberUtil.Int_valueOf(request, "DeviceId");
            InsertTime = NumberUtil.Long_valueOf(request, "InsertTime") * 1000l;// 毫秒级别
        } catch (Exception e) {
            // 数据畸变，返回null
            logger.error(e.getMessage());
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
        
        StringBuffer sb = new StringBuffer();
        sb.append("point2:CompanyId:" + CompanyId + ",DeviceId:" + DeviceId + ",InsertTime:"
                + DateUtil.formatToHHMMSS(InsertTime) + "\n");
        logger.info("consuming : monitordata, " + DeviceId + ", " + DateUtil.formatToHHMMSS(InsertTime));// 已被转换为毫秒级别

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


        Voltage voltage = new Voltage();
        addFamilyU(put, request, sb, voltage);// monitortable columnFamily U
        Current current = new Current();
        addFamilyI(put, request, sb, current);// monitortable columnFamily I
        addFamilyP(put, request, sb, voltage, current);// monitortable columnFamily P
        addFamilyD(put, request, sb);// monitortable columnFamily D
        addDemand(request, InsertTime, put);
        
        List<Put> puts = new ArrayList<Put>();
        puts.add(put);
        return puts;
    }

	private void addDemand(Map<String, String[]> request, long InsertTime, Put put) {
		float ctr = 0f;
        float ptr = 0f;
        long timesecond = -1;
        try {
			ctr = NumberUtil.Float_valueOf(request, "CT");
			ptr = NumberUtil.Float_valueOf(request, "PT");
			timesecond = NumberUtil.Long_valueOf(request,"InsertTime");
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
        
		// 需量独立为一个put
        byte[] FpQualifier = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
                25, 26, 27, 28 };
        // WARN HOKO传过来的数据为kW
        float FPdemand = NumberUtil.Float_valueOf(request, "FPdemand");
        float BPdemand = NumberUtil.Float_valueOf(request, "BPdemand");
        float FQdemand = NumberUtil.Float_valueOf(request, "FQdemand");
        float BQdemand = NumberUtil.Float_valueOf(request, "BQdemand");

        long ts = 0;//权重

        if(timesecond != -1){
            if(timesecond % 3600 <= 15 * 60){//判断是否为整点之后的5分钟，把权重设置为1
                String deviceId = request.get("DeviceId")[0];
                if(deviceId != null && deviceIdTimestamp.get(deviceId) != null){
                    if(timesecond - deviceIdTimestamp.get(deviceId) <= 15 * 60){//15分钟之内不需要再设置，保证一个整点设置一次
                        deviceIdTimestamp.put(deviceId,timesecond);
                    }else{
                        ts = 1;
                        deviceIdTimestamp.put(deviceId, timesecond);
                    }
                }else{
                    ts = 1;
                    if(deviceId != null)
                        deviceIdTimestamp.put(deviceId,timesecond);
                }
            }
        }

        // WARN 历史遗留问题，存放到Hbase的需量单位为W
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[23] }, ts, Bytes.toBytes(FPdemand * 1000f));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[24] }, ts, Bytes.toBytes(BPdemand * 1000f));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[25] }, ts, Bytes.toBytes(FQdemand * 1000f));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[26] }, ts, Bytes.toBytes(BQdemand * 1000f));
        put.add(Bytes.toBytes("P"), new byte[]{FpQualifier[27]}, ts, Bytes.toBytes(InsertTime));
	}

    // 辅助方法，添加colunmfamily U组的qualifier进put
    private void addFamilyU(Put put, Map<String, String[]> request, StringBuffer sb, Voltage voltage) {
    	float ptr = 0f;
        try {
			ptr = NumberUtil.Float_valueOf(request, "PT");
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
        
        float Ua = NumberUtil.Float_valueOf(request, "Ua") * ptr;
        float Ub = NumberUtil.Float_valueOf(request, "Ub") * ptr;
        float Uc = NumberUtil.Float_valueOf(request, "Uc") * ptr;
        float U0 = NumberUtil.Float_valueOf(request, "U0") * ptr;
        
        voltage.ua = Ua;
        voltage.ub = Ub;
        voltage.uc = Uc;

        byte[] FuQualifier = { 1, 2, 3, 4 };
        put.add(Bytes.toBytes("U"), new byte[] { FuQualifier[0] }, 0, Bytes.toBytes(Ua));
        put.add(Bytes.toBytes("U"), new byte[] { FuQualifier[1] }, 0, Bytes.toBytes(Ub));
        put.add(Bytes.toBytes("U"), new byte[] { FuQualifier[2] }, 0, Bytes.toBytes(Uc));
        put.add(Bytes.toBytes("U"), new byte[] { FuQualifier[3] }, 0, Bytes.toBytes(U0));

    }

    // 辅助方法，添加colunmfamily I组的qualifier进put
    private void addFamilyI(Put put, Map<String, String[]> request, StringBuffer sb, Current current) {
    	float ctr = 0f;
        try {
			ctr = NumberUtil.Float_valueOf(request, "CT");
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
        
        float Ia = NumberUtil.Float_valueOf(request, "Ia") * ctr;
        float Ib = NumberUtil.Float_valueOf(request, "Ib") * ctr;
        float Ic = NumberUtil.Float_valueOf(request, "Ic") * ctr;
        current.ia = Ia;
        current.ib = Ib;
        current.ic = Ic;
        float I0 = NumberUtil.Float_valueOf(request, "I0") * ctr;

        byte[] FiQualifier = { 1, 2, 3, 4 };
        put.add(Bytes.toBytes("I"), new byte[] { FiQualifier[0] }, 0, Bytes.toBytes(Ia));
        put.add(Bytes.toBytes("I"), new byte[] { FiQualifier[1] }, 0, Bytes.toBytes(Ib));
        put.add(Bytes.toBytes("I"), new byte[] { FiQualifier[2] }, 0, Bytes.toBytes(Ic));
        put.add(Bytes.toBytes("I"), new byte[] { FiQualifier[3] }, 0, Bytes.toBytes(I0));
    }

    // 辅助方法，添加colunmfamily P组的qualifier进put
    private void addFamilyP(Put put, Map<String, String[]> request, StringBuffer sb, Voltage voltage, Current current) {
    	float ctr = 0f;
        float ptr = 0f;
        try {
			ctr = NumberUtil.Float_valueOf(request, "CT");
			ptr = NumberUtil.Float_valueOf(request, "PT");
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
        
    	// 汉光传过来的功率是kW和kWh
        float Pa = NumberUtil.Float_valueOf(request, "Pa") * ctr * ptr;
        float Pb = NumberUtil.Float_valueOf(request, "Pb") * ctr * ptr;
        float Pc = NumberUtil.Float_valueOf(request, "Pc") * ctr * ptr;
        float P0 = NumberUtil.Float_valueOf(request, "P0") * ctr * ptr;
        float Qa = NumberUtil.Float_valueOf(request, "Qa") * ctr * ptr;
        float Qb = NumberUtil.Float_valueOf(request, "Qb") * ctr * ptr;
        float Qc = NumberUtil.Float_valueOf(request, "Qc") * ctr * ptr;
        float Q0 = NumberUtil.Float_valueOf(request, "Q0") * ctr * ptr;
        float Sa = NumberUtil.Float_valueOf(request, "Sa") * ctr * ptr;
        float Sb = NumberUtil.Float_valueOf(request, "Sb") * ctr * ptr;
        float Sc = NumberUtil.Float_valueOf(request, "Sc") * ctr * ptr;
        float S0 = NumberUtil.Float_valueOf(request, "S0") * ctr * ptr;
        float Ca = NumberUtil.Float_valueOf(request, "Ca");
        float Cb = NumberUtil.Float_valueOf(request, "Cb");
        float Cc = NumberUtil.Float_valueOf(request, "Cc");
        float PFT = NumberUtil.Float_valueOf(request, "PFT");

        byte[] FpQualifier = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
                25, 26, 27, 28 };
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[0] }, 0, Bytes.toBytes(Pa));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[1] }, 0, Bytes.toBytes(Pb));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[2] }, 0, Bytes.toBytes(Pc));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[3] }, 0, Bytes.toBytes(P0));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[8] }, 0, Bytes.toBytes(Qa));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[9] }, 0, Bytes.toBytes(Qb));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[10] }, 0, Bytes.toBytes(Qc));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[11] }, 0, Bytes.toBytes(Q0));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[12] }, 0, Bytes.toBytes(Sa));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[13] }, 0, Bytes.toBytes(Sb));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[14] }, 0, Bytes.toBytes(Sc));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[15] }, 0, Bytes.toBytes(S0));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[16] }, 0, Bytes.toBytes(Ca));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[17] }, 0, Bytes.toBytes(Cb));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[18] }, 0, Bytes.toBytes(Cc));
        put.add(Bytes.toBytes("P"), new byte[] { FpQualifier[21] }, 0, Bytes.toBytes(PFT));
    }

    // 辅助方法，添加colunmfamily D组的qualifier进put
    private void addFamilyD(Put put, Map<String, String[]> request, StringBuffer sb) {
    	float ctr = 0f;
        float ptr = 0f;
        try {
			ctr = NumberUtil.Float_valueOf(request, "CT");
			ptr = NumberUtil.Float_valueOf(request, "PT");
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
        
        double Epi = NumberUtil.Double_valueOf(request, "Epi") * ctr * ptr;
        double Epo = NumberUtil.Double_valueOf(request, "Epo") * ctr * ptr;
        double EQind = NumberUtil.Double_valueOf(request, "EQind") * ctr * ptr;
        double EQcap = NumberUtil.Double_valueOf(request, "EQcap") * ctr * ptr;
        float F = NumberUtil.Float_valueOf(request, "F");
        double PeakEpi = NumberUtil.Double_valueOf(request, "PeakEpi") * ctr * ptr;
        double FlatEpi = NumberUtil.Double_valueOf(request, "FlatEpi") * ctr * ptr;
        double ValleyEpi = NumberUtil.Double_valueOf(request, "ValleyEpi") * ctr * ptr;

        sb.append("Epi:" + Epi + "\n");
        sb.append("PeakEpi:" + PeakEpi + "\n");
        sb.append("FlatEpi:" + FlatEpi + "\n");
        sb.append("ValleyEpi:" + ValleyEpi + "\n");

        byte[] FdQualifier = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21 };
        put.add(Bytes.toBytes("D"), new byte[] { FdQualifier[0] }, 0, Bytes.toBytes(Epi));
        put.add(Bytes.toBytes("D"), new byte[] { FdQualifier[7] }, 0, Bytes.toBytes(Epo));
        put.add(Bytes.toBytes("D"), new byte[] { FdQualifier[8] }, 0, Bytes.toBytes(EQind));
        put.add(Bytes.toBytes("D"), new byte[] { FdQualifier[9] }, 0, Bytes.toBytes(EQcap));
        put.add(Bytes.toBytes("D"), new byte[] { FdQualifier[10] }, 0l, Bytes.toBytes(F));
        put.add(Bytes.toBytes("D"), new byte[] { FdQualifier[11] }, 0, Bytes.toBytes(PeakEpi));
        put.add(Bytes.toBytes("D"), new byte[] { FdQualifier[12] }, 0, Bytes.toBytes(FlatEpi));
        put.add(Bytes.toBytes("D"), new byte[] { FdQualifier[13] }, 0, Bytes.toBytes(ValleyEpi));
    }

    @Override
    public boolean put(final List<Put> put) {
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

    /**
     * backup the terminal settings to local file
     * 
     * @param deviceid
     * @param backupstr
     */
    @SuppressWarnings("unused")
    private void terminalSettingsBackup(Short deviceid, String backupstr) {
        File directory = new File("");// 设定为当前文件夹
        try {
            String folder = directory.getAbsolutePath();
            String path = folder + File.separator + "backup" + File.separator + "monitordata" + File.separator
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

    private void terminalSettingsBackup2(Short deviceid, String backupstr) {
        File directory = new File("");// 设定为当前文件夹
        try {
            String folder = directory.getAbsolutePath();
            String path = folder + File.separator + "backup2" + File.separator + "monitordata" + File.separator
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
