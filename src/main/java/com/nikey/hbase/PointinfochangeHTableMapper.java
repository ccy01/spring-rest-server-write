package com.nikey.hbase;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import com.nikey.util.JsonUtil;
import com.nikey.util.LogJsonUtil;
import com.nikey.util.ServiceHelper;

public class PointinfochangeHTableMapper implements HTableMapper {
    
    @Override
    public List<Put> convertParameterMapToPut(Map<String, String[]> requestMap) {
        String json = requestMap.get("json")[0];
        if(json != null && json.lastIndexOf("}") != -1) {
            json = new String(json.getBytes(Charset.forName("iso8859-1")), Charset.forName("UTF-8"));
            Map<String, Object> jsonMap = JsonUtil.fromJsonToHashMap(json.substring(0, json.lastIndexOf("}")+1));
            if(jsonMap != null && jsonMap.get("data") != null) {
                @SuppressWarnings("unchecked")
                List<Object> list = (List<Object>) jsonMap.get("data"); // LinkedTreeMap
                try {
                    ServiceHelper.instance().getGroupInfoService().insertPchage(list);
                } catch (Exception e) {
                    e.printStackTrace();
                    LogJsonUtil.errorJsonFileRecord(getClass().getSimpleName(), "insert error", JsonUtil.toJson(list));
                }
            }
        }
        return null;
    }

    @Override
    public boolean put(List<Put> put) {
        return true;
    }

    @Override
    public ResultScanner getResultScannerWithParameterMap(Scan scan) {
        return null;
    }

}
