package com.nikey.hokoemc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyuncs.exceptions.ClientException;
import com.nikey.bean.AlarmCounter;
import com.nikey.bean.AlarmReceiver;
import com.nikey.bean.AlarmsetThreshold;
import com.nikey.bean.HokoPointinfo;
import com.nikey.bean.JsonConvert;
import com.nikey.bean.MaxDemand;
import com.nikey.bean.PowerDemand;
import com.nikey.bean.PowerInfo;
import com.nikey.bean.RealtimeAlarm;
import com.nikey.hbase.HbaseTablePool;
import com.nikey.http.HttpClientRequestUtil;
import com.nikey.http.NikeyConsts;
import com.nikey.thread.HokoHttpWorker;
import com.nikey.thread.HokoemcWorkerQueue;
import com.nikey.thread.WorkQueue;
import com.nikey.util.DateUtil;
import com.nikey.util.HostNameUtil;
import com.nikey.util.JsonUtil;
import com.nikey.util.NumberUtil;
import com.nikey.util.PropUtil;
import com.nikey.util.RedisUtil;
import com.nikey.util.SendSmsUtil;
import com.nikey.util.ServiceHelper;
import com.nikey.util.TimeConvertUtil;

import redis.clients.jedis.Jedis;

/**
 * 汉光容转需工作线程
 *
 * @author JayzeeZhang ccy
 * @date 2018年3月27日
 */
public class HokoemcPostThread extends Thread {

    private String MAXDEAMND_ = "maxdemand_";
    private String FPDEMANDMMAX_ = "fpdemandmmax_";
    private String UPDATEDEMAND_ = "updatedemand_";
    private String HOKO_THREHOLD_ = "demand_threshold_";
    private String ALARM_SMS = "alarm-sms-";

    private String HOKO_POINTINFO = "hoko_pointinfo";
    private String HOKO_POWERINFO = "hoko_powerinfo";

    private int DAY_TIME_TYPE = 0;
    private int MONTH_TIME_TYPE = 1;

    private int FAULT_EVENT_ID = 11014;
    private int WARN_EVENT_ID = 21008;
    private Map<Integer, String> eventIdMapsToEventName = new HashMap<>();

    /**
     * e.g 公司ID映射到手机号码，多个以逗号分隔 2 -> 13430389072,13800000000
     */
    private Map<Integer, Set<String>> companyIdMapsToPhoneNumber = new HashMap<>();//
    private Set<Integer> companyIdSet = new HashSet<>();

    private Long DATA_CALL_INTERVAL = 60 * 1000L;
    private Long MINI_SLEEP_INTERVAL = 10 * 1000L;
    private Long TASK_SLEEP_TIME = 60 * 1000L;

    private Jedis jedis = RedisUtil.getInstance();

    private static final Logger logger = LoggerFactory.getLogger(HokoemcPostThread.class);

    private static final HokoemcPostThread instance = new HokoemcPostThread();

    public static HokoemcPostThread instance() {
        return instance;
    }

    private final int THREADNUMS = PropUtil.getInt("http_thread_num");
    private final ExecutorService httpExec = Executors.newFixedThreadPool(THREADNUMS);//这里修改线程池个数以便调整性能

    private HokoemcPostThread() {

        for (int i = 0; i < THREADNUMS; i++) {
            httpExec.execute(new HokoHttpWorker(httpExec, this::work));
        }
        if (HostNameUtil.isCapacityToDemandServer()) this.start();
    }

    @Override
    public void run() {

        Map<Integer, HokoPointinfo> deviceIdMapsToPointinfo = new HashMap<>();
        List<HokoPointinfo> hokoPointinfos = loadHokoPointinfos(deviceIdMapsToPointinfo);
        eventIdMapsToEventName = loadAlarmTypes();
        if (hokoPointinfos == null)
            return;

        Long taskTime = System.currentTimeMillis();
        Long lastCall = System.currentTimeMillis();
        Long lastCheck = System.currentTimeMillis();

        while (!Thread.currentThread().isInterrupted() && !WorkQueue.instance().getStopWorking()) {
            logger.debug("---com.nikey.hokoemc.HokoemcThread.work() run---");

            if (!DateUtil.isSameDay(taskTime, System.currentTimeMillis())) {//不是同一天
                deviceIdMapsToPointinfo.clear();
                hokoPointinfos = loadHokoPointinfos(deviceIdMapsToPointinfo);
                if (hokoPointinfos == null) return;
            }

            try {
                if (System.currentTimeMillis() - taskTime > TASK_SLEEP_TIME) {
                    RedisUtil.set(jedis, "currentTime", String.valueOf(System.currentTimeMillis()));
                    taskTime = System.currentTimeMillis();
                }

                if (System.currentTimeMillis() - lastCall > DATA_CALL_INTERVAL) {
                    for (HokoPointinfo hoko : hokoPointinfos) {

                        HokoemcWorkerQueue.instance().increment();//标志位增加
                        HokoemcWorkerQueue.instance().put(hoko);

                    }

                    long start = System.currentTimeMillis();
                    if (!HokoemcWorkerQueue.instance().isComplete()) {
                        HokoemcWorkerQueue.instance().await();
                    }
                    logger.info("阻塞的时间是" + (System.currentTimeMillis() - start));

                    // 检测是否有新增监测点
                    Map<String, String> kv = RedisUtil.hgetAll(jedis, HOKO_POINTINFO);
                    if (kv != null) {
                        Set<String> keySet = kv.keySet();
                        if (keySet != null && keySet.size() > 0) {
                            logger.info("detect pointinfo change : {}", JsonUtil.toJson(kv));
                            for (String key : keySet) {
                                String arr[] = key.split(",");
                                if (arr.length == 2) {
                                    Integer companyId = Integer.valueOf(arr[0]);
                                    Integer deviceId = Integer.valueOf(arr[1]);
                                    if (deviceIdMapsToPointinfo.containsKey(deviceId)) { // already exist
                                        continue;
                                    } else {
                                        String json = kv.get(key);
                                        try {
                                            Map<String, Object> item = JsonUtil.fromJsonToHashMap(json);
                                            if (item.get("contact_mac") != null) {
                                                HokoPointinfo hoko = new HokoPointinfo();
                                                hoko.setCompanyId(companyId);
                                                hoko.setDeviceId(deviceId);
                                                hoko.setMcode(item.get("contact_mac").toString());
                                                hoko.setHoko("044", "guanx"); // TODO what a dirty hard code here ...
                                                hokoPointinfos.add(hoko);

                                                deviceIdMapsToPointinfo.put(deviceId, hoko); // add mapping
                                                RedisUtil.hdel(jedis, HOKO_POINTINFO, key);
                                            }
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // 检测是否有新增电源
                    Map<String, String> keyValue = RedisUtil.hgetAll(jedis, HOKO_POWERINFO);
                    if (keyValue != null) {
                        Set<String> keySet = keyValue.keySet();
                        if (keySet != null && keySet.size() > 0) {
                            logger.info("detect powerinfo change : {}", JsonUtil.toJson(keyValue));
                            for (String key : keySet) {
                                String json = keyValue.get(key);
                                Map<String, Object> item = JsonUtil.fromJsonToHashMap(json);
                                if (item.get("is_group") != null && item.get("device_id") != null) {
                                    int is_group = NumberUtil.intValue(item.get("is_group"));
                                    int device_id = NumberUtil.intValue(item.get("device_id"));
                                    String arr[] = key.split(",");

                                    if (is_group == 0 && arr.length == 2) { // 目前仅处理监测点的情况
                                        int power_id = Integer.valueOf(arr[1]);
                                        if (deviceIdMapsToPointinfo.containsKey(device_id)) {
                                            deviceIdMapsToPointinfo.get(device_id).setPowerId(power_id);
                                            RedisUtil.hdel(jedis, HOKO_POWERINFO, key);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // 检测报警人是否被更新
                    // 假设companyIdMapsToPhoneNumber中没有存储companyid为5的手机号码（预加载时，数据库确实没有companyid5的手机号码），客户此时通过WEB为companyid5新增了手机号码，会有什么问题？
                    for (Integer companyId : companyIdSet) {
                        checkAlarmPhone(companyId);
                    }

                    lastCall = System.currentTimeMillis();

                } else if (System.currentTimeMillis() - lastCheck > MINI_SLEEP_INTERVAL) {
                    boolean reload = false;

                    for (HokoPointinfo hoko : hokoPointinfos) {
                        if (hoko.getMaxDemandRealTime() == null
                                || (DateUtil.isSameMonth(hoko.getMaxDemandRealTime(), System.currentTimeMillis())
                                && DateUtil.isSameMonth(lastCheck, System.currentTimeMillis()))) {
                            // 检测需量是否被WEB更新
                            String value = RedisUtil.get(UPDATEDEMAND_ + hoko.getPowerId(), jedis);
                            if (value != null) {
                                logger.info("detect demand contract value change, key {}, value {}",
                                        UPDATEDEMAND_ + hoko.getPowerId(), value);
                                hoko.setMaxDemandContract(Float.valueOf(value));
                                updateMaxDemandToRedis(new MaxDemand(hoko.getPowerId(),
                                        DateUtil.formatToHHMMSS(hoko.getMaxDemandRealTime()), hoko.getMaxDemandReal(),
                                        hoko.getMaxDemandContract()));
                                RedisUtil.del(UPDATEDEMAND_ + hoko.getPowerId(), jedis);
                            }
                            // 检测是否有阈值调整
                            String json = RedisUtil.get(HOKO_THREHOLD_ + hoko.getPowerId(), jedis);
                            if (json != null) {
                                logger.info("detect threshold change : {}", JsonUtil.toJson(json));
                                Map<String, Object> item = JsonUtil.fromJsonToHashMap(json);
                                if (item.get("is_group") != null) {
                                    int is_group = NumberUtil.intValue(item.get("is_group"));
                                    if (is_group == 0) {
                                        if (item.get("warn") != null)
                                            hoko.setWarnPercent(Float.valueOf(item.get("warn").toString()));
                                        if (item.get("fault") != null)
                                            hoko.setFaultPercent(Float.valueOf(item.get("fault").toString()));
                                        RedisUtil.del(HOKO_THREHOLD_ + hoko.getPowerId(), jedis);
                                    }
                                }
                            }

                            // 跨天清空计数 & 更新内存
                            Date lastTime = null;
                            if (hoko.getDayFaultCounter() != null && hoko.getDayFaultCounter().getInsertTime() != null)
                                lastTime = hoko.getDayFaultCounter().getInsertTime();
                            if (hoko.getDayWarnCounter() != null && hoko.getDayWarnCounter().getInsertTime() != null) {
                                if (lastTime == null
                                        || hoko.getDayWarnCounter().getInsertTime().getTime() < lastTime.getTime())
                                    lastTime = hoko.getDayWarnCounter().getInsertTime(); // 取最小时间
                            }

                            if ((lastTime != null
                                    && !DateUtil.isSameDay(lastTime.getTime(), System.currentTimeMillis()))
                                    || !DateUtil.isSameDay(lastCheck, System.currentTimeMillis())) {
                                logger.info("next day arrives ...");
                                clearDayCounter(hoko);
                            }
                        } else {
                            logger.info("hoko : " + JsonUtil.toJson(hoko));
                            logger.info("lastCheck : " + DateUtil.formatToHHMMSS(lastCheck) + ", now : "
                                    + DateUtil.formatToHHMMSS(System.currentTimeMillis()));
                            logger.info("next month arrives ...");
                            // 跨月需量处理
                            String key = MAXDEAMND_ + hoko.getPowerId();
                            RedisUtil.del(key, jedis);
                            ServiceHelper.instance().getHokoCapacityToDemandService().getKeyValueMapper().del(key);
                            reload = true;

                            clearMonthCounter(hoko);
                        }
                    }

                    if (reload) {
                        hokoPointinfos = loadHokoPointinfos(deviceIdMapsToPointinfo);
                        if (hokoPointinfos == null)
                            return;
                    }

                    lastCheck = System.currentTimeMillis();
                } else {
                    Thread.sleep(MINI_SLEEP_INTERVAL);
                }
            } catch (InterruptedException e) {
                logger.error("com.nikey.hokoemc.HokoemcThread.work() sleep error!/n {}", e.getMessage());
            }
        }
    }

    /**
     * 检测报警人手机在Redis中是否变更
     */
    private void checkAlarmPhone(Integer companyId) {
        Map<String, String> phoneJson = RedisUtil.hgetAll(jedis, ALARM_SMS + companyId);

        if (phoneJson != null && !phoneJson.isEmpty()) {
            logger.info("变更报警人信息:{} companyId:{}", phoneJson, companyId);
            Set<String> phoneSet = companyIdMapsToPhoneNumber.get(companyId);
            // 如果原来数据库的sms表里没有这个companyId
            if (phoneSet == null) {
                phoneSet = new HashSet<>();
            }
            for (String phone : phoneJson.keySet()) {
                String newOrDel = phoneJson.get(phone);
                if (newOrDel.equals("1")) {
                    phoneSet.add(phone);
                    companyIdMapsToPhoneNumber.put(companyId, phoneSet);
                    logger.info("添加Redis手机号成功:{} companyId:{}", phone, companyId);
                    RedisUtil.hdel(jedis, ALARM_SMS + companyId, phone); // 检测完成后删除RedisUtil没用了的数据
                } else if (newOrDel.equals("0")) {
                    // 如果是0代表删除
                    phoneSet.remove(phone);
                    companyIdMapsToPhoneNumber.put(companyId, phoneSet);
                    RedisUtil.hdel(jedis, ALARM_SMS + companyId, phone);
                    logger.info("删除Redis手机号成功:{} companyId:{}", phone, companyId);
                }
            }
        }
    }

    private void clearDayCounter(HokoPointinfo hoko) {
        ServiceHelper.instance().getHokoCapacityToDemandService().getAlarmMapper()
                .clearSpecificDayCounter(hoko.getPowerId(), hoko.getCompanyId());
        hoko.setDayFaultCounter(null);
        hoko.setDayWarnCounter(null);
    }

    private void clearMonthCounter(HokoPointinfo hoko) {
        ServiceHelper.instance().getHokoCapacityToDemandService().getAlarmMapper()
                .clearSpecificMonthCounter(hoko.getPowerId(), hoko.getCompanyId());
        hoko.setMonthFaultCounter(null);
        hoko.setMonthWarnCounter(null);
    }

    /**
     * 加载警告类型
     */
    private Map<Integer, String> loadAlarmTypes() {
        List<AlarmsetThreshold> getAlarmTypeNames = null;
        getAlarmTypeNames = ServiceHelper.instance().getHokoCapacityToDemandService().getAlarmMapper()
                .getAlarmTypeNames();
        if (getAlarmTypeNames != null && getAlarmTypeNames.size() > 0) {
            for (AlarmsetThreshold alarmsetThreshold : getAlarmTypeNames) {
                eventIdMapsToEventName.put(alarmsetThreshold.getAlarmEventId(), alarmsetThreshold.getAlarmTypeName());
            }
        }
        return eventIdMapsToEventName;
    }

    /**
     * 加载汉光监测点的所有信息
     */
    private List<HokoPointinfo> loadHokoPointinfos(Map<Integer, HokoPointinfo> deviceIdMapsToPointinfo) {
        List<HokoPointinfo> hokoPointinfos = null;
        List<AlarmReceiver> alarmReceivers = null;

        try {
            alarmReceivers = ServiceHelper.instance().getHokoCapacityToDemandService().getAlarmMapper()
                    .getPhoneNumber();
            hokoPointinfos = ServiceHelper.instance().getHokoCapacityToDemandService().getTbPowerMapper()
                    .getHokoPointinfos();
            List<PowerDemand> powerDemands = ServiceHelper.instance().getHokoCapacityToDemandService()
                    .getPowerDemands();
            List<PowerInfo> powerInfos = ServiceHelper.instance().getHokoCapacityToDemandService().getTbPowerMapper()
                    .getPowerInfos();
            List<AlarmsetThreshold> alarmsetThresholds = ServiceHelper.instance().getHokoCapacityToDemandService()
                    .getAlarmMapper().getAlarmsetThresholds();
            List<AlarmCounter> alarmCounters = ServiceHelper.instance().getHokoCapacityToDemandService()
                    .getAlarmMapper().getAlarmCounters();

            // 整合手机号码数据
            List<Map<Integer, String>> listMap = new ArrayList<>();
            if (alarmReceivers != null && alarmReceivers.size() > 0) {
                for (AlarmReceiver phoneNumber : alarmReceivers) {
                    Map<Integer, String> map = new HashMap<>();
                    map.put(phoneNumber.getCompanyId(), phoneNumber.getPhoneNumber());
                    listMap.add(map);
                }
                for (Map<Integer, String> m : listMap) {
                    Iterator<Integer> it = m.keySet().iterator();
                    while (it.hasNext()) {
                        Integer key = it.next();
                        if (!companyIdMapsToPhoneNumber.containsKey(key)) {
                            Set<String> newSet = new HashSet<>();
                            newSet.add(m.get(key));
                            companyIdMapsToPhoneNumber.put(key, newSet);
                        } else {
                            companyIdMapsToPhoneNumber.get(key).add(m.get(key));
                        }
                    }
                }
            }

            //加载时除去暂时不需要检测的点。
            List<String> ignoreDeviceIds = null;
            String igDeviceIds = PropUtil.getString("ignoreDeviceId");
            if (igDeviceIds != null && !igDeviceIds.equals("")) {
                String t[] = igDeviceIds.split(",");
                ignoreDeviceIds = Arrays.asList(t);
            }


            if (hokoPointinfos != null && hokoPointinfos.size() > 0) {
                for (Iterator<HokoPointinfo> ite = hokoPointinfos.iterator(); ite.hasNext(); ) {

                    HokoPointinfo point = ite.next();
                    if (ignoreDeviceIds != null && ignoreDeviceIds.size() > 0) {
                        if (ignoreDeviceIds.contains(point.getDeviceId().toString())) {
                            logger.info("忽略检测的deviceId号 + " + point.getDeviceId());
                            ite.remove();
                            continue;
                        }
                    }
                    logger.info("余留下的deviceid号 + " + point.getDeviceId());
                    companyIdSet.add(point.getCompanyId());
                    deviceIdMapsToPointinfo.put(point.getDeviceId(), point);
                    point.setHoko("044", "guanx"); // TODO what a dirty hard code here ...
                }

                if (powerInfos != null && powerInfos.size() > 0) {
                    for (PowerInfo power : powerInfos) {
                        if (!power.getIsGroup()) {
                            HokoPointinfo point = deviceIdMapsToPointinfo.get(power.getDeviceId()); // 初始化map
                            if (point != null)
                                point.setPowerId(power.getPowerId());
                        }
                    }
                }

                if (powerDemands != null && powerDemands.size() > 0) {
                    for (PowerDemand demand : powerDemands) {
                        if (!demand.getIsGroup()) {
                            HokoPointinfo point = deviceIdMapsToPointinfo.get(demand.getDeviceId());
                            if (point != null)
                                point.setMaxDemandContract(demand.getMaxDemand());
                        }
                    }
                }

                if (alarmsetThresholds != null && alarmsetThresholds.size() > 0) {
                    for (AlarmsetThreshold alarmset : alarmsetThresholds) {
                        HokoPointinfo point = deviceIdMapsToPointinfo.get(alarmset.getDeviceId());
                        if (point != null) {
                            if (alarmset.getEventId() == FAULT_EVENT_ID) {
                                if (alarmset.getMax() != null)
                                    point.setFaultPercent(alarmset.getMax());
                            } else if (alarmset.getEventId() == WARN_EVENT_ID) {
                                if (alarmset.getMax() != null)
                                    point.setWarnPercent(alarmset.getMax());
                            }
                        }
                    }
                }

                if (alarmCounters != null && alarmCounters.size() > 0) {
                    // 初始化月、天计数
                    for (AlarmCounter alarmCounter : alarmCounters) {
                        // insertTime错误则清除 & 更新内存
                        if (alarmCounter.getTimeType() != null && alarmCounter.getInsertTime() != null) {
                            if (alarmCounter.getTimeType() == DAY_TIME_TYPE) { // 日
                                if (!DateUtil.isSameDay(System.currentTimeMillis(),
                                        alarmCounter.getInsertTime().getTime())) {
                                    ServiceHelper.instance().getHokoCapacityToDemandService().getAlarmMapper()
                                            .clearSpecificDayCounter(alarmCounter.getPowerId(),
                                                    alarmCounter.getCustomerId());
                                    continue;
                                }
                            } else if (alarmCounter.getTimeType() == MONTH_TIME_TYPE) { // 月
                                if (!DateUtil.isSameMonth(System.currentTimeMillis(),
                                        alarmCounter.getInsertTime().getTime())) {
                                    ServiceHelper.instance().getHokoCapacityToDemandService().getAlarmMapper()
                                            .clearSpecificMonthCounter(alarmCounter.getPowerId(),
                                                    alarmCounter.getCustomerId());
                                    continue;
                                }
                            }
                            HokoPointinfo point = deviceIdMapsToPointinfo.get(alarmCounter.getDeviceId());
                            if (point != null) {
                                if (alarmCounter.getTimeType() == DAY_TIME_TYPE) { // 日
                                    if (alarmCounter.getEventId() == WARN_EVENT_ID) {
                                        point.setDayWarnCounter(alarmCounter);
                                    } else if (alarmCounter.getEventId() == FAULT_EVENT_ID) {
                                        point.setDayFaultCounter(alarmCounter);
                                    }
                                } else if (alarmCounter.getTimeType() == MONTH_TIME_TYPE) { // 月
                                    if (alarmCounter.getEventId() == WARN_EVENT_ID) {
                                        point.setMonthWarnCounter(alarmCounter);
                                    } else if (alarmCounter.getEventId() == FAULT_EVENT_ID) {
                                        point.setMonthFaultCounter(alarmCounter);
                                    }
                                }
                            }
                        }
                    }
                }

                for (HokoPointinfo point : hokoPointinfos) {
                    MaxDemand max = new MaxDemand(point.getPowerId(), null, null, point.getMaxDemandContract());
                    updateMaxDemandToRedis(max);
                    // 更新实际最大需量到内存
                    if (max.getReal() != null && max.getTime() != null) {
                        long time = DateUtil.parseHHMMSSToDate(max.getTime()).getTime();
                        if (DateUtil.isSameMonth(time, System.currentTimeMillis())) {
                            point.setMaxDemandReal(max.getReal());
                            point.setMaxDemandRealTime(time);
                        }
                    }

                    logger.info("load pointinfo : {}", JsonUtil.toJson(point));
                }

            }
        } catch (Exception e) {
            logger.info("load hoko pointinfo error, exit ...");
            e.printStackTrace();
            System.exit(0);
        }
        return hokoPointinfos;
    }

    /**
     * 设置最大需量到redis，key/value的规约参考：http://192.168.1.3:4999/index.php?s=/12&page_id=120
     */
    private void updateMaxDemandToRedis(MaxDemand maxDemand) {
        if (maxDemand.getPowerId() != null) {
            String key = MAXDEAMND_ + maxDemand.getPowerId();

            if (maxDemand.getReal() == null && maxDemand.getTime() == null) { // 初始化redis和内存
                String json = RedisUtil.get(key, jedis);
                if (json == null)
                    json = ServiceHelper.instance().getHokoCapacityToDemandService().getKeyValueMapper().get(key); // 尝试从数据库取得缓存
                if (json != null) {
                    MaxDemand cache = (MaxDemand) JsonUtil.fromJson(json, MaxDemand.class);
                    if (cache != null && cache.getReal() != null && cache.getTime() != null) {
                        // redis有实际最大需量
                        maxDemand.setReal(cache.getReal());
                        maxDemand.setTime(cache.getTime());
                    }
                }
            }

            if (maxDemand.getReal() == null && maxDemand.getTime() == null && maxDemand.getContract() == null)
                return;

            Map<String, Object> map = new HashMap<>();
            if (maxDemand.getTime() != null && maxDemand.getReal() != null) {
                map.put("time", maxDemand.getTime());
                map.put("real", maxDemand.getReal());
            }
            if (maxDemand.getContract() != null)
                map.put("contract", maxDemand.getContract());
            String value = JsonUtil.toJson(map);
            RedisUtil.set(jedis, key, value);
            ServiceHelper.instance().getHokoCapacityToDemandService().getKeyValueMapper().put(key, value);
            logger.info("set to redis, key : {}, value : {}", key, value);
        }
    }

    private void work(HokoPointinfo pinfo) {

        String PrjCode = pinfo.getPrjCode();
        String magName = pinfo.getMagName();
        String Mcode = pinfo.getMcode();

        // 1. 监测点信息组织到paramMap中
        Map<String, String> paraMap = new HashMap<String, String>();
        paraMap.put("PrjCode", PrjCode);
        paraMap.put("magName", magName);
        paraMap.put("Mcode", Mcode);
        // 2. 请求HOKO的数据
        String json = HttpClientRequestUtil.sendRequest("POST", paraMap, NikeyConsts.BASE_REST_URL);
        // 3.检查返回的json
        logger.info("请求参数：" + JsonUtil.toJson(paraMap));
        logger.info("当前时间：" + DateUtil.formatToHHMMSS(System.currentTimeMillis()) + " " + pinfo.getDeviceId() + "返回的json：" + json);
        Map<String, Object> hashMap = null;
        try {
            hashMap = JsonUtil.fromJsonToHashMap(json);
        } catch (Exception e1) {
            logger.error("返回的json转换失败，请求的参数不正确！！PrjCode：{}，magName：{}，Mcode：{}", PrjCode, magName, Mcode);
            e1.printStackTrace();
            return;
        }

        // 4.转换的hashMap判断
        if (hashMap == null || hashMap.isEmpty()) {
            return;
        }

        String Code = (String) hashMap.get("Code");
        // 汉光接口RCount返回被封装成Double，非String
        Double RCount = (Double) hashMap.get("RCount");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> objSetMapList = (List<Map<String, Object>>) hashMap.get("ObjSet");
        // 5.判断Code及RCount及ObjSet
        if (Code == null || (!"0".equals(Code)) || RCount == null || RCount == 0l || objSetMapList == null
                || objSetMapList.isEmpty()) {
            return;
        }

        // 6.遍历及转换构造成符合WorkQueue.put的格式
        for (Map<String, Object> objSetMap : objSetMapList) {
            // 装载数据map
            Map<String, String[]> postMap = new LinkedHashMap<String, String[]>();
            Map<String, String[]> postMapFPdemandMMax = new LinkedHashMap<String, String[]>();
            // 转换存储在map
            for (Map.Entry<String, Object> entry : objSetMap.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue().toString();// value均为String，toString即可
                postMap.put(key, new String[]{value});
            }
            // 空，不加入，跳到下一个
            if (postMap.isEmpty()) {
                return;
            }
            // 加入必要的列：htable,CompanyId,DeviceId,InsertTime(long,秒值，非毫秒)
            postMap.put("htable", new String[]{"monitordatav2"});
            postMap.put("CompanyId", new String[]{pinfo.getCompanyId().toString()});
            postMap.put("DeviceId", new String[]{pinfo.getDeviceId().toString()});

            // 插入时间
            String MTime = objSetMap.get("MTime").toString();
            Long dataTime = DateUtil.parseHHMMSSToDate(MTime.replace("/", "-")).getTime();

            // 正向有功需量
            if (objSetMap.get("FPdemand") != null && objSetMap.get("FPdemandMMaxTime") != null
                    && objSetMap.get("FPdemandMMax") != null) {
                Float FPdemand = Double.valueOf(objSetMap.get("FPdemand").toString()).floatValue(); // HOKO需量单位为kW
                Float FPdemandMMax = Double.valueOf(objSetMap.get("FPdemandMMax").toString()).floatValue(); // HOKO需量单位为kW
                String FPdemandMMaxTime = objSetMap.get("FPdemandMMaxTime").toString() + ":00";
                Long dataMaxTime = DateUtil.parseHHMMSSToDate(FPdemandMMaxTime).getTime();
                Float redisMaxDemandReal = pinfo.getMaxDemandReal();

                logger.info("FPdemand: {} FPdemandMMaxTime: {} pinfo.getMaxDemandReal {} pinfo.getMaxDemandTime {} ", FPdemandMMax, FPdemandMMaxTime, pinfo.getMaxDemandReal(), pinfo.getMaxDemandRealTime());
                if (pinfo.getPowerId() != null && FPdemandMMax >= 0) {
                    // 更新最大需量
                    if (pinfo.getMaxDemandReal() == null || FPdemandMMax > pinfo.getMaxDemandReal()) {
                        if (DateUtil.isSameMonth(dataMaxTime, System.currentTimeMillis())) {
                            logger.info("pinfo.getMaxDemandReal() == null || FPdemandMMax > pinfo.getMaxDemandReal()...");
                            // 最大需量更新则写hbase
                            postMapFPdemandMMax.put("htable", new String[]{"Fpdemandmmax"});
                            postMapFPdemandMMax.put("CompanyId", new String[]{pinfo.getCompanyId().toString()});
                            postMapFPdemandMMax.put("DeviceId", new String[]{pinfo.getDeviceId().toString()});
                            postMapFPdemandMMax.put("FPdemandMMax", new String[]{FPdemandMMax.toString()});
                            postMapFPdemandMMax.put("FPdemandMMaxTime", new String[]{FPdemandMMaxTime});
                            String value = "{\"real\":" + FPdemandMMax + "," + "\"time\":\"" + FPdemandMMaxTime + "\"}";
                            RedisUtil.set(jedis, FPDEMANDMMAX_ + pinfo.getPowerId(), value);

                            updateMaxDemandToRedis(new MaxDemand(pinfo.getPowerId(), DateUtil.formatToHHMMSS(dataMaxTime),
                                    FPdemandMMax, pinfo.getMaxDemandContract()));
                            pinfo.setMaxDemandReal(FPdemandMMax);
                            pinfo.setMaxDemandRealTime(dataMaxTime);

                        } else {
                            // 跨月需量处理
                            pinfo.setMaxDemandReal(null);
                            pinfo.setMaxDemandRealTime(null);

                        }
                    }
                    // 检测是否触发预警报警
                    if (pinfo.getMaxDemandContract() != null && pinfo.getMaxDemandReal() != null) {
                        float max = mathMax(pinfo.getMaxDemandReal(), pinfo.getMaxDemandContract());  // 如若最大需量已超了合同需量，则合同需量就要变为此最大需量

                        //发生了最大需量变化，并且此最大需量已经超出报警范围
                        if (pinfo.getFaultPercent() != null && FPdemandMMax > redisMaxDemandReal
                                && FPdemandMMax > max * pinfo.getFaultPercent() / 100) { // 95 / 100 => 0.95 => %95
                            logger.info("发生最大需量变化，并且此最大需量已经超出报警范围");
                            logger.info("deviceid {} arises demand fault, demand is {}", pinfo.getDeviceId(), FPdemandMMax);
                            // jtb 2018-4-16 调用短信API发送短信报警信息
                            Integer companyId = pinfo.getCompanyId();
                            String customer = pinfo.getCompanyForShort();
                            String pointinfo = pinfo.getDeviceName();
                            String eventName = eventIdMapsToEventName.get(FAULT_EVENT_ID);
                            float beforeThrehold = max * pinfo.getFaultPercent() / 100;
                            String threhold = String.valueOf(beforeThrehold);
                            String beforeFPdemand = String.valueOf(FPdemandMMax);

                            // 先检测Redis是否有更改报警接收人
                            checkAlarmPhone(companyId);

                            if (companyIdMapsToPhoneNumber != null && !companyIdMapsToPhoneNumber.isEmpty()) {
                                // 此companyId在数据库必须有报警短信接收人
                                if (companyIdMapsToPhoneNumber.get(companyId) != null) {
                                    String phoneNumber = StringUtils
                                            .strip(companyIdMapsToPhoneNumber.get(companyId).toString(), "[]");
                                    System.out.println("手机号有：" + phoneNumber);
                                    JsonConvert jc = new JsonConvert();
                                    jc.setCustomer(customer);
                                    jc.setPointinfo(pointinfo);
                                    jc.setTime(FPdemandMMaxTime);
                                    jc.setEvent(eventName);
                                    jc.setThrehold(threhold);
                                    jc.setValue(beforeFPdemand);
                                    String sendSmsJson = JsonUtil.toJson(jc);
                                    try {
                                        SendSmsUtil.sendSms(sendSmsJson, phoneNumber);
                                    } catch (ClientException e) {
                                        logger.error("ClientException");
                                        e.printStackTrace();
                                    }
                                } else {
                                    logger.error("此companyId在数据库没有短信接收人!");
                                }
                            }

                            // 1. 写实时预警报警表（SAVE UPDATE)
                            ServiceHelper.instance().getHokoCapacityToDemandService().getAlarmMapper()
                                    .insertRealtimeAlarm(new RealtimeAlarm(pinfo.getPowerId(), new Date(dataMaxTime),
                                            FAULT_EVENT_ID, FPdemandMMax.toString(), pinfo.getCompanyId(), 2,
                                            max * pinfo.getFaultPercent() / 100));
                            // 2. 更新日、月计数 & 内存
                            if (pinfo.getDayFaultCounter() == null) {
                                pinfo.setDayFaultCounter(new AlarmCounter(pinfo.getPowerId(), new Date(dataMaxTime),
                                        FAULT_EVENT_ID, 1, pinfo.getCompanyId(), DAY_TIME_TYPE, pinfo.getDeviceId()));
                            } else {
                                pinfo.getDayFaultCounter().increaseCount();
                                pinfo.getDayFaultCounter().setInsertTime(new Date(dataMaxTime));
                            }
                            ServiceHelper.instance().getHokoCapacityToDemandService().getAlarmMapper()
                                    .insertAlarmCounter(pinfo.getDayFaultCounter());

                            if (pinfo.getMonthFaultCounter() == null) {
                                pinfo.setMonthFaultCounter(new AlarmCounter(pinfo.getPowerId(), new Date(dataMaxTime),
                                        FAULT_EVENT_ID, 1, pinfo.getCompanyId(), MONTH_TIME_TYPE, pinfo.getDeviceId()));
                            } else {
                                pinfo.getMonthFaultCounter().increaseCount();
                                pinfo.getMonthFaultCounter().setInsertTime(new Date(dataMaxTime));
                            }
                            ServiceHelper.instance().getHokoCapacityToDemandService().getAlarmMapper()
                                    .insertAlarmCounter(pinfo.getMonthFaultCounter());
                        } else if (pinfo.getFaultPercent() != null && FPdemand > max * pinfo.getFaultPercent() / 100) {

                            logger.info("deviceid {} arises demand fault, demand is {}", pinfo.getDeviceId(), FPdemand);
                            // jtb 2018-4-16 调用短信API发送短信报警信息
                            Integer companyId = pinfo.getCompanyId();
                            String customer = pinfo.getCompanyForShort();
                            String pointinfo = pinfo.getDeviceName();
                            String dataTimeToString = String.valueOf(dataTime);
                            String dateTimeToDateFormat = TimeConvertUtil.stampToDate(dataTimeToString);
                            String eventName = eventIdMapsToEventName.get(FAULT_EVENT_ID);
                            float beforeThrehold = max * pinfo.getFaultPercent() / 100;
                            String threhold = String.valueOf(beforeThrehold);
                            String beforeFPdemand = String.valueOf(FPdemand);

                            // 先检测Redis是否有更改报警接收人
                            checkAlarmPhone(companyId);

                            if (companyIdMapsToPhoneNumber != null && !companyIdMapsToPhoneNumber.isEmpty()) {
                                // 此companyId在数据库必须有报警短信接收人
                                if (companyIdMapsToPhoneNumber.get(companyId) != null) {
                                    String phoneNumber = StringUtils
                                            .strip(companyIdMapsToPhoneNumber.get(companyId).toString(), "[]");
                                    System.out.println("手机号有：" + phoneNumber);
                                    JsonConvert jc = new JsonConvert();
                                    jc.setCustomer(customer);
                                    jc.setPointinfo(pointinfo);
                                    jc.setTime(dateTimeToDateFormat);
                                    jc.setEvent(eventName);
                                    jc.setThrehold(threhold);
                                    jc.setValue(beforeFPdemand);
                                    String sendSmsJson = JsonUtil.toJson(jc);

                                    try {
                                        SendSmsUtil.sendSms(sendSmsJson, phoneNumber);
                                    } catch (ClientException e) {
                                        logger.error("ClientException");
                                        e.printStackTrace();
                                    }
                                } else {
                                    logger.error("此companyId在数据库没有短信接收人!");
                                }
                            }
                            // 1. 写实时预警报警表（SAVE UPDATE)
                            ServiceHelper.instance().getHokoCapacityToDemandService().getAlarmMapper()
                                    .insertRealtimeAlarm(new RealtimeAlarm(pinfo.getPowerId(), new Date(dataTime),
                                            FAULT_EVENT_ID, FPdemand.toString(), pinfo.getCompanyId(), 2,
                                            max * pinfo.getFaultPercent() / 100));
                            // 2. 更新日、月计数 & 内存
                            if (pinfo.getDayFaultCounter() == null) {
                                pinfo.setDayFaultCounter(new AlarmCounter(pinfo.getPowerId(), new Date(dataTime),
                                        FAULT_EVENT_ID, 1, pinfo.getCompanyId(), DAY_TIME_TYPE, pinfo.getDeviceId()));
                            } else {
                                pinfo.getDayFaultCounter().increaseCount();
                                pinfo.getDayFaultCounter().setInsertTime(new Date(dataTime));
                            }
                            ServiceHelper.instance().getHokoCapacityToDemandService().getAlarmMapper()
                                    .insertAlarmCounter(pinfo.getDayFaultCounter());

                            if (pinfo.getMonthFaultCounter() == null) {
                                pinfo.setMonthFaultCounter(new AlarmCounter(pinfo.getPowerId(), new Date(dataTime),
                                        FAULT_EVENT_ID, 1, pinfo.getCompanyId(), MONTH_TIME_TYPE, pinfo.getDeviceId()));
                            } else {
                                pinfo.getMonthFaultCounter().increaseCount();
                                pinfo.getMonthFaultCounter().setInsertTime(new Date(dataTime));
                            }
                            ServiceHelper.instance().getHokoCapacityToDemandService().getAlarmMapper()
                                    .insertAlarmCounter(pinfo.getMonthFaultCounter());

                        } else if (pinfo.getWarnPercent() != null && FPdemand > max * pinfo.getWarnPercent() / 100) {
                            logger.info("deviceid {} arises demand warn, demand is {}", pinfo.getDeviceId(), FPdemand);
                            // 后预警
                            // WARN_EVENT_ID
                            // threhold：max * pinfo.getWarnPercent() / 100
                            // jtb 2018-4-16 调用短信API发送短信「预警」信息
                            Integer companyId = pinfo.getCompanyId();
                            String customer = pinfo.getCompanyForShort();
                            String pointinfo = pinfo.getDeviceName();
                            String dataTimeToString = String.valueOf(dataTime);
                            String dateTimeToDateFormat = TimeConvertUtil.stampToDate(dataTimeToString);
                            String eventName = eventIdMapsToEventName.get(WARN_EVENT_ID);
                            float beforeThrehold = max * pinfo.getWarnPercent() / 100;
                            String threhold = String.valueOf(beforeThrehold);
                            String beforeFPdemand = String.valueOf(FPdemand);

                            // 先检测Redis是否有更改报警接收人
                            checkAlarmPhone(companyId);

                            if (companyIdMapsToPhoneNumber != null && !companyIdMapsToPhoneNumber.isEmpty()) {
                                // 此companyId在数据库必须有报警短信接收人
                                if (companyIdMapsToPhoneNumber.get(companyId) != null) {
                                    String phoneNumber = StringUtils
                                            .strip(companyIdMapsToPhoneNumber.get(companyId).toString(), "[]");
                                    System.out.println("手机号有：" + phoneNumber);
                                    JsonConvert jc = new JsonConvert();
                                    jc.setCustomer(customer);
                                    jc.setPointinfo(pointinfo);
                                    jc.setTime(dateTimeToDateFormat);
                                    jc.setEvent(eventName);
                                    jc.setThrehold(threhold);
                                    jc.setValue(beforeFPdemand);
                                    String sendSmsJson = JsonUtil.toJson(jc);

                                    try {
                                        SendSmsUtil.sendSms(sendSmsJson, phoneNumber);
                                    } catch (ClientException e) {
                                        logger.error("ClientException");
                                        e.printStackTrace();
                                    }
                                } else {
                                    logger.error("此companyId在数据库没有短信接收人!");
                                }

                            }

                            ServiceHelper.instance().getHokoCapacityToDemandService().getAlarmMapper()
                                    .insertRealtimeAlarm(new RealtimeAlarm(pinfo.getPowerId(), new Date(dataTime),
                                            WARN_EVENT_ID, FPdemand.toString(), pinfo.getCompanyId(), 2,
                                            max * pinfo.getWarnPercent() / 100));

                            if (pinfo.getDayWarnCounter() == null) {
                                pinfo.setDayWarnCounter(new AlarmCounter(pinfo.getPowerId(), new Date(dataTime),
                                        WARN_EVENT_ID, 1, pinfo.getCompanyId(), DAY_TIME_TYPE, pinfo.getDeviceId()));
                            } else {
                                pinfo.getDayWarnCounter().increaseCount();
                                pinfo.getDayWarnCounter().setInsertTime(new Date(dataTime));
                            }
                            ServiceHelper.instance().getHokoCapacityToDemandService().getAlarmMapper()
                                    .insertAlarmCounter(pinfo.getDayWarnCounter());

                            if (pinfo.getMonthWarnCounter() == null) {
                                pinfo.setMonthWarnCounter(new AlarmCounter(pinfo.getPowerId(), new Date(dataTime),
                                        WARN_EVENT_ID, 1, pinfo.getCompanyId(), MONTH_TIME_TYPE, pinfo.getDeviceId()));
                            } else {
                                pinfo.getMonthWarnCounter().increaseCount();
                                pinfo.getMonthWarnCounter().setInsertTime(new Date(dataTime));
                            }
                            ServiceHelper.instance().getHokoCapacityToDemandService().getAlarmMapper()
                                    .insertAlarmCounter(pinfo.getMonthWarnCounter());
                        }
                    }
                }
            } else {
                return;
            }

            String InsertTime = dataTime / 1000l + "";// 秒级别
            postMap.put("InsertTime", new String[]{InsertTime});

            if (System.currentTimeMillis() - dataTime > 5 * 60 * 1000l)
                return; // 超过5分钟，丢弃

            // 放入WorkQueue
            // if not stopWorking
            if (HostNameUtil.isCapacityToDemandServer()) {
                if (!WorkQueue.instance().getStopWorking() && HbaseTablePool.instance().getIsConnected()) {
                    int state = WorkQueue.instance().put(new HashMap<>(postMap));
                    if (state != 200) {// 数据有误
                        logger.error("加入队列失败，可能数据格式有误.请求的数据：{}", postMap);
                    }

                    if (postMapFPdemandMMax != null && !postMapFPdemandMMax.isEmpty()) {
                        int stateFPdemandMMax = WorkQueue.instance().put(new HashMap<>(postMapFPdemandMMax));
                        if (stateFPdemandMMax != 200) {
                            logger.error("postMapFPdemandMMax加入队列失败，可能数据格式有误.请求的数据：{}", postMapFPdemandMMax);
                        }
                    }

                    if (pinfo.getGroupId() != null) {
                        if (2001 == pinfo.getDeviceId()) {
                            postMap.put("htable", new String[]{"groupv2"});
                            postMap.put("DeviceId", new String[]{"2"}); // TODO groupid dirty hard code
                            state = WorkQueue.instance().put(postMap);
                            if (state != 200) {// 数据有误
                                logger.error("加入队列失败，可能数据格式有误.请求的数据：{}", postMap);
                            }
                        }
                    }
                } else {
                    logger.info("the WorkQueue or HbaseTablePool is stop working ...");
                }
            }
        }
    }

    public static float mathMax(Float maxDemandReal, Float maxDemandContract) {
        if (maxDemandReal == null) {
            return maxDemandContract;
        } else {
            float max = Math.max(maxDemandReal, maxDemandContract);
            return max;
        }
    }

    public static void main(String[] args) throws InterruptedException {
        HokoemcPostThread hpt = new HokoemcPostThread();
        Map<String, Object> map = new HashMap<>();
        map.put("device_id", 4001);
        map.put("is_group", 0);
        map.put("warn", 61);
        map.put("fault", 61);
        RedisUtil.set(hpt.jedis, hpt.HOKO_THREHOLD_ + "2", JsonUtil.toJson(map));

    }

}
