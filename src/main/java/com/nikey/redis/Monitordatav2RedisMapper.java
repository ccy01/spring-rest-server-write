package com.nikey.redis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nikey.util.DateUtil;
import com.nikey.util.JsonUtil;
import com.nikey.util.NumberUtil;
import com.nikey.util.RedisUtil;

import redis.clients.jedis.Jedis;

/**
 * Monitordatav2 redis实时数据业务
 * 
 * @author JayzeeZhang
 * @date 2018年1月26日
 */
public class Monitordatav2RedisMapper implements RedisMapper {
	
	Logger logger = LoggerFactory.getLogger(getClass());
	
	//此处已经new了一个jedis实例
	private Jedis jedis = RedisUtil.getInstance();
	
	private Long HOUR_SUB = 5 * 60 * 1000l; // 5分钟内视为整点
	
	/**
	 * HINT：使用assert需要配置IDE
	 */
	private void degreeColumnTest() {
		
		//重新new一个jedis
		jedis = new Jedis("192.168.1.2", 6379);
		
		Map<String, String[]> request = new HashMap<>();
		request.put("InsertTime", new String[] { DateUtil.parseHHMMSSToDate("2018-01-26 00:14:00").getTime() / 1000 + "" }); // 毫秒 => 秒
		request.put("CompanyId", new String[] {"2"});
		request.put("DeviceId", new String[] {"2001"});
		request.put("Epi", new String[] {"1"});
		request.put("EQind", new String[] {"1"});
		
		String key = "2/electricaldegrees/1";
		
		// 非整点
		RedisUtil.del(key, jedis);
		convertParameterMapToRedis(request);
		assert(RedisUtil.get(key, jedis) == null);
		
		// 凌晨
		request.put("InsertTime", new String[] { DateUtil.parseHHMMSSToDate("2018-01-26 00:04:00").getTime() / 1000 + "" });
		convertParameterMapToRedis(request);
		assert(RedisUtil.get(key, jedis) != null);
		
		// 整点非凌晨 + redis有数据 + 跨天
		request.put("InsertTime", new String[] { DateUtil.parseHHMMSSToDate("2018-01-29 00:04:00").getTime() / 1000 + "" });
		convertParameterMapToRedis(request);
		String json = RedisUtil.get(key, jedis);
		assert(json != null);
		System.out.println("expect time 2018-01-29 00:04:00, real : " + json);
		RedisUtil.del(key, jedis);
		
		// 整点非凌晨 + redis无数据
		request.put("InsertTime", new String[] { DateUtil.parseHHMMSSToDate("2018-01-29 10:04:00").getTime() / 1000 + "" });
		convertParameterMapToRedis(request);
		assert(json != null);
		
		// 整点非凌晨 + redis有数据 + 同一天 + 有小时中断
		request.put("InsertTime", new String[] { DateUtil.parseHHMMSSToDate("2018-01-29 14:04:00").getTime() / 1000 + "" });
		convertParameterMapToRedis(request);
		json = RedisUtil.get(key, jedis);
		assert(json != null);
		System.out.println("expect array has null, real : " + json);
		
		// 整点非凌晨 + redis有数据 + 同一天 + 无小时中断
		request.put("InsertTime", new String[] { DateUtil.parseHHMMSSToDate("2018-01-29 15:04:00").getTime() / 1000 + "" });
		convertParameterMapToRedis(request);
		json = RedisUtil.get(key, jedis);
		assert(json != null);
		System.out.println("expect last value not null, real : " + json);
		
		// 时间乱序
		request.put("InsertTime", new String[] { DateUtil.parseHHMMSSToDate("2018-01-29 13:04:00").getTime() / 1000 + "" });
		convertParameterMapToRedis(request);
		
		// 行度错乱
		request.put("InsertTime", new String[] { DateUtil.parseHHMMSSToDate("2018-01-29 17:04:00").getTime() / 1000 + "" });
		request.put("Epi", new String[] {"0"});
		convertParameterMapToRedis(request);
	}
	
	/**
	 * HINT：使用assert需要配置IDE
	 * request在这里给出
	 */
	private void powerCurveTest() {
		Map<String, String[]> request = new HashMap<>();
		request.put("InsertTime", new String[] { DateUtil.parseHHMMSSToDate("2018-01-26 00:14:00").getTime() / 1000 + "" }); // 毫秒 => 秒
		request.put("CompanyId", new String[] {"2"});
		request.put("DeviceId", new String[] {"2001"});
		request.put("P0", new String[] {"1"});//TODO new String[] {"1"}...  
		
		String key = "2/power/1"; // => ${companyid}/power/${deviceid} 

		RedisUtil.del(key, jedis);  
		convertParameterMapToRedis(request);
		String json = RedisUtil.get(key, jedis); 
		assert(json != null);
		System.out.println(JsonUtil.toJson(json));
		
		request.put("InsertTime", new String[] { DateUtil.parseHHMMSSToDate("2018-01-26 00:15:00").getTime() / 1000 + "" }); // 毫秒 => 秒
		convertParameterMapToRedis(request);
		json = RedisUtil.get(key, jedis);
		assert(json != null);
		System.out.println(JsonUtil.toJson(json));
		
		request.put("InsertTime", new String[] { DateUtil.parseHHMMSSToDate("2018-01-26 10:15:00").getTime() / 1000 + "" }); // 毫秒 => 秒
		convertParameterMapToRedis(request);
		json = RedisUtil.get(key, jedis);
		assert(json != null);
		System.out.println(JsonUtil.toJson(json));
		
		request.put("InsertTime", new String[] { DateUtil.parseHHMMSSToDate("2018-01-26 10:25:00").getTime() / 1000 + "" }); // 毫秒 => 秒
		convertParameterMapToRedis(request);
		json = RedisUtil.get(key, jedis);
		assert(json != null);
		System.out.println(JsonUtil.toJson(json));
	}

	@Override
	public void convertParameterMapToRedis(Map<String, String[]> request) {
		degreeColumn(request);
		powerCurve(request);
		voltageCurve(request);
		electricalcurrentCurve(request);
		demandCurve(request);
		powerfactorCurve(request);
		logger.debug("convertParameterMapToRedis()完成:{}",request);
	}

	/**
	 *
	 * key : company/electricaldegrees/device  			//electrical degrees：电的度数
	 * value : {
				"monthlystartEpi":xxx,（月开始行度）// 已废弃
				"monthlyEpi":xxx,（月电度）// 已废弃
				"monthlyCount":xx,(月电度计数，记录当前整点缓存是缓存当月第几天的数据) // 已废弃
				
				"dailystartEpi":xxx,(凌晨行度)
				"lastEpiOn":xxx(上个整点的缓存),
				"lastEqindOn":xxx（上个整点的缓存）,
				
				"dailyEpi":xxx,（日电度）
				"epi":[xxx(24)],（正向有功小时电度）
				"eqind":[xxx(24)],（正向无功小时电度）
				"dailyCount":xx(日电度计数,记录当前整点缓存是缓存当天第几个小时的数据),
				"lastTime":xx(时间的毫秒值)
				}
	 * 
	 * @param request
	 */
	private void degreeColumn(Map<String, String[]> request) {
		float ctr = 0f;
        float ptr = 0f;
        try {
			ctr = NumberUtil.Float_valueOf(request, "CT"); //获取request的key("CT")对应的value
			ptr = NumberUtil.Float_valueOf(request, "PT");
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
        
		if (request.get("CompanyId") == null || request.get("DeviceId") == null || request.get("InsertTime") == null ||
				request.get("Epi") == null || request.get("EQind") == null) {
			return;
		}
		
		int CompanyId = NumberUtil.Short_valueOf(request, "CompanyId");
		int deviceID = NumberUtil.Int_valueOf(request, "DeviceId");
		long insertTime = NumberUtil.Long_valueOf(request, "InsertTime") * 1000l; // 秒 => 毫秒
		String key = CompanyId + "/electricaldegrees/" + (deviceID % 1000); // deviceID仅保留个、十、百位
		
		// 隔天：清除数据
		Map<String, Object> jsonMap = timeJudge(insertTime, key);
		if (jsonMap == null) return; // 连接redis出错或数据乱序
		
		boolean isEmpty = jsonMap.isEmpty();
		
		// 凌晨整点：凌晨行度、月行度更新
		if (! DateUtil.isHour(insertTime, HOUR_SUB)) return; // 非整点数据直接忽略
		boolean isDay = DateUtil.isMorning(insertTime, HOUR_SUB); // 返回true表示凌晨0点
		if (isDay) {
			jsonMap.put("dailystartEpi", ctr * ptr * NumberUtil.Double_valueOf(request, "Epi")); 
			jsonMap.put("dailyEpi", 0);
			
			jsonMap.put("lastEpiOn", ctr * ptr * NumberUtil.Double_valueOf(request, "Epi"));
			jsonMap.put("lastEqindOn", ctr * ptr * NumberUtil.Double_valueOf(request, "EQind"));
			jsonMap.put("epi", new double[0]);
			jsonMap.put("eqind", new double[0]);
			jsonMap.put("dailyCount", 0);
			jsonMap.put("lastTime", insertTime);
			
			if (jsonMap != null) RedisUtil.set(jedis, key, JsonUtil.toJson(jsonMap));
			return;
		}
		
		// 其他整点（非凌晨）
		int frontHourIndex = DateUtil.getHourIndex(insertTime);
		if (isEmpty) {
			Double value [] = new Double[frontHourIndex];
			for (int i = 0; i < value.length; i++) {
				value[i] = null;
			}
			
			jsonMap.put("lastEpiOn", ctr * ptr * NumberUtil.Double_valueOf(request, "Epi"));
			jsonMap.put("lastEqindOn", ctr * ptr * NumberUtil.Double_valueOf(request, "EQind"));
			jsonMap.put("epi", value); 
			jsonMap.put("eqind", value); 
			jsonMap.put("dailyCount", frontHourIndex);
			jsonMap.put("lastTime", insertTime); 
		} else {
			// 已在timeJudge保证了time的正确性
			int lastHourIndex = NumberUtil.intValue(jsonMap.get("dailyCount"));
			if (frontHourIndex == lastHourIndex) return; // 防止重复插入电度
			
			int sub = frontHourIndex - lastHourIndex;
			@SuppressWarnings("unchecked")
			List<Double> epi = (List<Double>) jsonMap.get("epi");
			@SuppressWarnings("unchecked")
			List<Double> eqind = (List<Double>) jsonMap.get("eqind");
			
			// 中断小时补null（需考虑非连续的可能）
			for (int i = 0; i < sub - 1; i++) {
				epi.add(null);
				eqind.add(null);
			}
			// 计算电量
			double realEpi = ctr * ptr * NumberUtil.Double_valueOf(request, "Epi") - NumberUtil.doubleValue(jsonMap.get("lastEpiOn"));
			if (realEpi < 0) {
				logger.error(String.format("negative degree, request is %s, redis json is %s", 
						JsonUtil.toJson(request), JsonUtil.toJson(jsonMap)));
				return; // 行度是递增的，进来的数据有误
			}
			epi.add(realEpi);
			eqind.add(ctr * ptr * NumberUtil.Double_valueOf(request, "EQind") - NumberUtil.doubleValue(jsonMap.get("lastEqindOn")));
			
			jsonMap.put("lastEpiOn", ctr * ptr * NumberUtil.Double_valueOf(request, "Epi"));
			jsonMap.put("lastEqindOn", ctr * ptr * NumberUtil.Double_valueOf(request, "EQind"));
			jsonMap.put("epi", epi);
			jsonMap.put("eqind", eqind);
			jsonMap.put("dailyCount", frontHourIndex);
			jsonMap.put("lastTime", insertTime);
			
			if (jsonMap.get("dailystartEpi") != null) {
				jsonMap.put("dailyEpi", ctr * ptr * NumberUtil.Double_valueOf(request, "Epi") - 
						NumberUtil.doubleValue(jsonMap.get("dailystartEpi")));
			}
		}
		
		if (jsonMap != null) RedisUtil.set(jedis, key, JsonUtil.toJson(jsonMap));
	}

	/**
	 * 收到数据的时间：insertTime
	 * 
	 * @param insertTime
	 * @param key
	 * @return
	 */
	private Map<String, Object> timeJudge(long insertTime, String key) {
		// 若进来的数据与当前不是同一天，直接删除key
		// 校验数据的合法性
		if (! DateUtil.isSameDay(insertTime, System.currentTimeMillis())) {
			RedisUtil.del(key, jedis); 
			return null; // 没有达到计算实时数据的条件
		}
		
		String json = RedisUtil.get(key, jedis);
		if (json != null) {
			if ("-1".equals(json)) return null; // 连接redis出错
			Map<String, Object> jsonMap = JsonUtil.fromJsonToHashMap(json);
			
			// redis无数据
			if (jsonMap == null || jsonMap.get("lastTime") == null) return new HashMap<>();
			// redis有数据
			else { 
				// redis的数据是否已过期
				long lastTime = NumberUtil.longValue(jsonMap.get("lastTime")); // 将lastTime拆箱成基本类型
				if (! DateUtil.isSameDay(lastTime, System.currentTimeMillis())) {
					RedisUtil.del(key, jedis);
					return new HashMap<>();
				}
				
				// 新收到的数据 vs 上一次收到的数据
				if (insertTime <= lastTime) {
					if (insertTime < lastTime) logger.info("degree data time error, json : " + json);
					return null; // 时间应为递增，当前数据时间比历史数据时间还小，意味收到乱序数据，丢弃之
				}
				
				return jsonMap;
			}
		} else {
			return new HashMap<>();
		}
	}
	
	public static void main(String[] args) {
		Monitordatav2RedisMapper mapper = new Monitordatav2RedisMapper();
		mapper.degreeColumnTest();
		mapper.powerCurveTest();
	}

	/**
	 * key : company/powerfactor/device
	 * value : {"PFT":[[insert_time,pft]], "lastTime":"xxx"}
	 * 
	 * @param request
	 */
	private void powerfactorCurve(Map<String, String[]> request) {
		String params [] = { "PFT" };
		String type = "powerfactor";
		
		curveData(request, params, type, null);
	}

	/**
	 * key : company/demandcurve/device
	 * {"demand":[[insert_time,ia](多个)], "lastTime":"xxxx"}
	 * 
	 * @param request
	 */
	private void demandCurve(Map<String, String[]> request) {
	    request.put("demand", request.get("FPdemand"));
		// WARN 实时需量的单位为kW，HOKO传过来的数据即为kW
		String params [] = { "demand" };
		String type = "demandcurve";
		
		curveData(request, params, type, null);
	}

	/**
	 * key : company/power/device
	 * value : {"A":[[insert_time,pa](多个)],
				"B":[[insert_time,pb](多个)],
				"C":[[insert_time,pc](多个)],
				"P0":[[insert_time,p0](多个)],
				"S0":[[insert_time,s0](多个)],
				"lastTime":"xxxx"}"
	 * 
	 * @param request
	 */
	private void powerCurve(Map<String, String[]> request) {
		String params [] = { "P0","S0" };
		String type = "power";
		
		float ctr = 0f;
        float ptr = 0f;
        try {
			ctr = NumberUtil.Float_valueOf(request, "CT");
			ptr = NumberUtil.Float_valueOf(request, "PT");
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		
		curveData(request, params, type, ctr * ptr);
	}
	
	/**
	 * 
	 * 	request.put("InsertTime", new String[] { DateUtil.parseHHMMSSToDate("2018-01-26 00:14:00").getTime() / 1000 + "" }); // 毫秒 => 秒
		request.put("CompanyId", new String[] {"2"});
		request.put("DeviceId", new String[] {"2001"});
		request.put("P0", new String[] {"1"});
	 * 	String key = "2/power/1";
	 * 
	 * @param request 
	 * @param params  String params [] = { "P0" };
	 * @param type    String type = "power";
	 * @param multi   ctr * ptr
	 */
	private void curveData(Map<String, String[]> request, String[] params, String type, Float multi) {
		if (request.get("CompanyId") == null || request.get("DeviceId") == null || request.get("InsertTime") == null) {
			return;
		}
		for (String par : params) {
			if (request.get(par) == null) return;
		}
		
		int CompanyId = NumberUtil.Short_valueOf(request, "CompanyId");
		int deviceID = NumberUtil.Int_valueOf(request, "DeviceId");
		long insertTime = NumberUtil.Long_valueOf(request, "InsertTime") * 1000l; // 秒 => 毫秒
		String key = CompanyId + "/" + type + "/" + (deviceID % 1000); // deviceID仅保留个、十、百位
		
		// 隔天：清除数据
		Map<String, Object> jsonMap = timeJudge(insertTime, key); 
		if (jsonMap == null) return; // 连接redis出错或数据乱序
		boolean isEmpty = jsonMap.isEmpty(); 

		if (isEmpty) {
			for (String par : params) {
				jsonMap.put(par, new Object[] { new Object[] {insertTime, NumberUtil.Float_valueOf(request, par) * (multi == null ? 1 : multi)} });	
			}
		} else {
			// lastTime judge
			Long lastTime = NumberUtil.longValue(jsonMap.get("lastTime"));
			boolean addNull = false;
			if (insertTime - lastTime > 20 * 60 * 1000l) {
				addNull = true; // 数据超过20分钟没更新，设置断点
			} else if (insertTime - lastTime < 2 * 60 * 1000l) {
				return; // 2分钟更新一次redis
			}
			for (String par : params) {
				@SuppressWarnings("unchecked")
				List<List<Object>> list = (List<List<Object>>) jsonMap.get(par);

				if(list == null){
					list = new ArrayList<List<Object>>();//修复加入S0后的空指针异常
				}

				if (addNull) {
					list.add(Arrays.asList( new Object[] {insertTime - 1000l, null} ));
				}
				//超过2分钟但没超过20分钟
				list.add(Arrays.asList( new Object[] {insertTime, NumberUtil.Float_valueOf(request, par) * (multi == null ? 1 : multi)} ));
				jsonMap.put(par, list);
			}
		}
		
		if (jsonMap != null) {
			jsonMap.put("lastTime", insertTime);
			RedisUtil.set(jedis, key, JsonUtil.toJson(jsonMap));
		}
	}
	
    private void voltageCurve(Map<String, String[]> request) {
        String params [] = { "U0", "Ua", "Ub", "Uc" }; 
        String type = "voltage";
        
        float ptr = 0f;
        try {
            ptr = NumberUtil.Float_valueOf(request, "PT");
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        
        curveData(request, params, type, ptr);
    }
    
    private void electricalcurrentCurve(Map<String, String[]> request) {
        String params [] = { "I0", "Ia", "Ib", "Ic" }; 
        String type = "electricalcurrent";
        
        float ctr = 0f;
        try {
            ctr = NumberUtil.Float_valueOf(request, "CT");
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        
        curveData(request, params, type, ctr);
    }
}
