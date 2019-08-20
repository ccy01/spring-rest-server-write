package com.nikey.test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nikey.contactmac.MonitoryPointInfo;
import com.nikey.hbase.HbaseTablePool;
import com.nikey.http.HttpClientRequestUtil;
import com.nikey.http.NikeyConsts;
import com.nikey.thread.ThreadPoolManagerWrite;
import com.nikey.thread.WorkQueue;
import com.nikey.util.DateUtil;
import com.nikey.util.HostNameUtil;
import com.nikey.util.JsonUtil;


public class HokoHistoryDataTest {
    
    private static final Logger logger = LoggerFactory.getLogger(HokoHistoryDataTest.class);
    
//    private static String sTime = "2018-04-29 00:00:00";
//    private static String eTime = "2018-05-01 00:00:00";
    private static String prjCode = "044";
    private static String magName = "guanx";
    private static long daylong = 24 * 60 * 60 * 1000l;
    
//    private static String mCode = "90180011560100";
//    static String[] str = MonitoryPointInfo.getInstance().pointInfo(mCode);
//    static String companyId = str[0];
//    static String deviceId = str[1];
    
    public static void paragraph(String sTime, String eTime, String[] companyIdAndMcode, String deviceId) {
        
        String companyId = companyIdAndMcode[0];
        String mCode = companyIdAndMcode[1];
        // 把字符串转成时间戳
        long sTimeLong = DateUtil.parseHHMMSSToDate(sTime).getTime();
        long eTimeLong = DateUtil.parseHHMMSSToDate(eTime).getTime();
        
        long parTime = eTimeLong - sTimeLong;
        // 如果请求历史数据的时间间隔大于一天，则分天请求
        if (parTime > daylong) {
            // 计算有多少天
            long str =  parTime / daylong;
            // 如果不是整数天，则要 +1次请求
            if (parTime % daylong != 0) {
                str +=1;
            }
            for (long i = str; i > 0; i--) {
                // 结束时间 - 剩余被请求的天数 = 将要分段请求的截止日期
                String eTime2 = DateUtil.formatToHHMMSS(eTimeLong - (i - 1) * daylong);
                // 如果已经完成第一次请求了的就要改变请求的起始时间
                if (i != str) {
                    // 根据第几次请求来决定起始时间是哪个时间段
                    String eTime3 = DateUtil.formatToHHMMSS(eTimeLong - (i) * daylong);
                    work(eTime3, eTime2, companyId, mCode, deviceId);
                } else {
                    work(sTime, eTime2, companyId, mCode, deviceId);
                }
            }
        } else {
            work(sTime, eTime, companyId, mCode, deviceId);
        }
    }
    
    public static void work(String sTime, String eTime, String companyId, String mCode, String deviceId) {
    
        // 1.监测点信息组织到paramMap中
        Map<String, String> paramMap = new HashMap<>();
        paramMap.put("prjCode", prjCode);
        paramMap.put("magName", magName);
        paramMap.put("mCode", mCode);
        paramMap.put("sTime", sTime);
        paramMap.put("eTime", eTime);
        // 2.请求HOKO的数据
        String json = HttpClientRequestUtil.sendRequest("POST", paramMap, NikeyConsts.HISTORY_REST_URL);
        // 3.检查返回的json
        logger.info("请求的参数" + JsonUtil.toJson(paramMap));
//        logger.info(companyId + "返回的json" + json);
        Map<String, Object> jsonMap = null;
        try {
            jsonMap = JsonUtil.fromJsonToHashMap(json);
        } catch (Exception e) {
            logger.error("返回的Json参数转换失败，请求的参数出错！!prjCode：{}, magName：{}, mCode: {}", prjCode, magName, mCode);
            e.printStackTrace();
            return;
        }
        // 4. 转换的jsonMap判断
        if (jsonMap == null || jsonMap.isEmpty()) {
             return;
        }
        
        String Code = (String) jsonMap.get("Code");
        Double RCount = (Double) jsonMap.get("RCount");
        // 5.判断Code及RCount及ObjSet
        @SuppressWarnings("unchecked")
        List<Map<String, String>> objSetMapList = (List<Map<String, String>>) jsonMap.get("ObjSet");
        if (Code == null || !("0".equals(Code)) || RCount == null ||RCount == 0l || objSetMapList == null 
                || objSetMapList.isEmpty()) {
            return;
        }
        
        // 6.遍历及转换构造成功符合WorkQueue.put的格式
        for (Map<String, String> objSetMap : objSetMapList) {
            // 装载数据的postMap
            Map<String, String[]> postMap = new LinkedHashMap<String, String[]>();
            // 转换存储map
            for (Map.Entry<String, String> entry : objSetMap.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue().toString();// value均为String，toString即可
                postMap.put(key, new String[] {value});
            }
            //空，不加入，跳到下一个
            if (postMap.isEmpty()) {
                return;
            }
            // 加入必要的列：htable,CompanyId,DeviceId,InsertTime(long,秒值，非毫秒)
            postMap.put("htable", new String[] { "monitordatav2" });
            postMap.put("CompanyId", new String[] { companyId });
            postMap.put("DeviceId", new String[] { deviceId });
            
            // 插入时间
            String MTime = objSetMap.get("MTime").toString();
            Long dataTime = DateUtil.parseHHMMSSToDate(MTime.replace("/", "-")).getTime();
            String InsertTime = dataTime / 1000l + "";// 秒级别
            postMap.put("InsertTime", new String[] { InsertTime });

            // 放入WorkQueue
            // if not stopWorking
            if (HostNameUtil.isCapacityToDemandServer()) {
                if (!WorkQueue.instance().getStopWorking() && HbaseTablePool.instance().getIsConnected()) {
                    int state = WorkQueue.instance().put(new HashMap<>(postMap));
                    if (state != 200) {// 数据有误
                        logger.error("加入队列失败，可能数据格式有误.请求的数据：{}", postMap);
                    }
                }
            }
        }
    }
    public static void main(String[] deviceIds) {
        
        ThreadPoolManagerWrite.instance();
        
        String sTime = deviceIds[1];
        String eTime = deviceIds[2];
        long sTimeLong = DateUtil.parseHHMMSSToDate(sTime).getTime();
        long eTimeLong = DateUtil.parseHHMMSSToDate(eTime).getTime();
        
        long parTime = eTimeLong - sTimeLong;
        if (parTime < 0) {
            logger.error("起始时间必须小于终止时间");
            return;
        }
        if (!DateUtil.isValidDate(sTime) || !DateUtil.isValidDate(eTime) || sTime == null || eTime == null) {
            System.out.println("参数格式错误！Usage java -jar xxx.jar "
                    + "参数1（多个deviceID以','分隔） 参数2（开始的时间:eg:2018-04-29 00:00:00） 参数3（结束时间）");
            return;
        }
        
        String[] deviceIdStr = deviceIds[0].split(",");
        logger.info("传入的参数========="+deviceIdStr,sTime,eTime);
        for (String deviceId : deviceIdStr) {
            String[] deviceIdAndMcode = MonitoryPointInfo.getInstance().pointInfo(deviceId);
            paragraph(sTime, eTime,deviceIdAndMcode, deviceId);
        }
        logger.info("数据获取完成!!!!!!!!!!!!!!!!");
        
    }

}
