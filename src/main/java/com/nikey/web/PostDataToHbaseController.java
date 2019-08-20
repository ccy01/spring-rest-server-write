package com.nikey.web;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.nikey.hbase.HbaseTablePool;
import com.nikey.hokoemc.HokoemcPostThread;
import com.nikey.thread.ThreadPoolManagerWrite;
import com.nikey.thread.WorkQueue;
import com.nikey.util.DateUtil;
import com.nikey.util.JsonUtil;
import com.nikey.util.LogJsonUtil;
import com.nikey.util.PropUtil;

/**
 * @author jayzee
 * @date 25 Sep, 2014
 *	get post data from communcation,
 *  and put the data to the queue
 */
@Controller
@RequestMapping(value = "/PostDataToHbaseController")
public class PostDataToHbaseController {
	
	/**
	 * slfj
	 */
	Logger logger = LoggerFactory.getLogger(getClass());
	
	/**
	 * the thread pool starts working
	 * the hbase pool starts working
	 */
	public PostDataToHbaseController() {
		// 实例化线程池
		ThreadPoolManagerWrite.instance();
		// 实例化hbase连接
		HbaseTablePool.instance();
        HokoemcPostThread.instance();
	}
	
	/**
	 * accept post data from communcation service
	 * @date 25 Sep, 2014
	 * @param request
	 * @param response
	 * @return 200 or 503
	 */
	@RequestMapping(value = "/acceptPostDataDeprecate", method = RequestMethod.GET)
	public void acceptPostData(HttpServletRequest request,HttpServletResponse response) {
		String htable = null;
		htable = request.getParameter(PropUtil.getString("LIB_CURVE_VALUE_TYPE"));
        // 将post过来的数据 "深拷贝"
        Map<String, String[]> requestMap = new HashMap<String, String[]>(request.getParameterMap());
		if(htable != null) {
		    // data_type
			htable = htable.toLowerCase();
			
			// data_id
			String data_id = request.getParameter("DeviceId") != null ? 
			        request.getParameter("DeviceId") : request.getParameter("GroupId");
			if (data_id != null) {
	            // data_time
	            String data_time = request.getParameter("InsertTime") != null ?
	                    request.getParameter("InsertTime") : request.getParameter("HappenTime");
	            if (data_time == null) data_time = request.getParameter("DemandTime");
	            if (data_time != null) {
	                try {
	                    data_time = DateUtil.formatToHHMMSS(Long.valueOf(data_time) * 1000l);
	                } catch (Exception e) {}
	            }
	            // 调试输出及异常data_id丢弃
	            logger.info(String.valueOf(htable) + ", " + data_id + ", " + data_time);
	            if ("0".equals(data_id)) {
	                logger.info("data_id error : " + JsonUtil.toJson(requestMap));
	                response.setStatus(200);
	                return;
	            }
			} else {
	            // json类数据预处理
	            if (jsonPreHandle(response, htable, requestMap, "json") == 1) {
	                return;
	            }
	            if (jsonPreHandle(response, htable, requestMap, "jsonr") == 1) {
	                return;
	            }
			}
			
			// if not stopWorking
			if (!WorkQueue.instance().getStopWorking() && HbaseTablePool.instance().getIsConnected()) {
				int state = WorkQueue.instance().put(requestMap);
				response.setStatus(state);
			} else {
				logger.info("the program is stop working ...");
				response.setStatus(400);	
			}
		} else {
			response.setStatus(400);
		}
	}

    /**
     * json预处理
     * @param response
     * @param htable
     * @param requestMap
     * @param jkey
     * @return 1失败 0成功
     */
    private int jsonPreHandle(HttpServletResponse response, String htable,
            Map<String, String[]> requestMap, String jkey) {
        if(requestMap.get(jkey) != null && requestMap.get(jkey).length > 0) {
            String json = requestMap.get(jkey)[0];
            if(json != null && json.lastIndexOf("}") != -1 && json.indexOf("{") != -1) {
                if ("pointinfochange".equals(htable)) {
                    json = new String(json.getBytes(Charset.forName("iso8859-1")), Charset.forName("UTF-8")); // 中文编码处理                        
                }
                try {
                    String substr = getJsonString(json);
                    Map<String, Object> jmap = JsonUtil.fromJsonToHashMap(substr);
                    requestMap.put(jkey, new String[]{ substr }); // set the correct json back
                    logJsonData(htable, jmap);
                } catch (Exception e) {
                    LogJsonUtil.errorJsonFileRecord(getClass().getSimpleName(), "malformed json", json);
                    response.setStatus(400);
                    return 1;
                }
            }
        }
        return 0;
    }

    /**
     * malform json prehandle
     * @param json
     * @return
     */
    private String getJsonString(String json) {
		int counter = 3; // 推进3次
    	int from = json.indexOf("{");
    	int to = json.lastIndexOf("}") + 1;
    	while (from >= 0 && to > from && counter > 0) {
    		counter--;
    		String jv = json.substring(from, to);
    		try {
    			JsonUtil.fromJsonToHashMap(jv);
    			return jv;
			} catch (Exception e) {
				json = json.substring(from, to - 1); // 向后推进一个字符
				from = json.indexOf("{");
				to = json.lastIndexOf("}") + 1;
			}
    	}
    	return null;
	}

	/**
     * 打印json类数据的调试信息
     * @param htable
     * @param jmap
     */
    private void logJsonData(String htable, Map<String, Object> jmap) {
        StringBuffer sb = new StringBuffer();
        if ("pointinfochange".equals(htable)) {
            sb.append(htable);
        } else {
            if (jmap.get("data_type") != null) {
                sb.append(jmap.get("data_type").toString());
                Object data_id = jmap.get("device_id") != null ? jmap.get("device_id") : jmap.get("group_id");
                if (data_id != null) {
                    sb.append(", " + data_id.toString());
                    Object data_time = null;
                    if (jmap.get("insert_time") != null) {
                        data_time = jmap.get("insert_time");
                    } else if (jmap.get("happen_time") != null) {
                        data_time = jmap.get("happen_time");
                    } else if (jmap.get("change_time") != null) {
                        data_time = jmap.get("change_time");
                    }
                    if (data_time != null) {
                        try {
                            Long time = 1000l * Long.valueOf(data_time.toString());
                            sb.append(", " + DateUtil.formatToHHMMSS(time));
                        } catch (Exception e) {
                            try {
                            	Long time = 1000l * Double.valueOf(data_time.toString()).longValue();
                                sb.append(", " + DateUtil.formatToHHMMSS(time));
							} catch (Exception e2) {}
                        }
                    }
                }
            }
        }
        if (sb.length() != 0) {
            logger.info(sb.toString());
        }
    }

}
