package com.nikey.service;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.nikey.util.ServiceHelper;

@Service
public class KilowService
{
    
    /**
     * slfj
     */
    Logger logger = LoggerFactory.getLogger(getClass());
    
    public KilowService() {
    	ServiceHelper.instance().setKilowService(this);
    }
    
    private Map<Short, Object[]> cache = new HashMap<Short, Object[]>();
    
    public Object[] getRelationMap(Short id) {
        synchronized (cache) {
            if(cache.get(id) != null) {
                return cache.get(id);
            }
        }
        return null;
    }
    public Boolean putrelationMap(Short id,Object[] params) {
        boolean flag=false;
        try {
            if(params != null) {
                synchronized (cache) {
                    cache.put(id, params);
                    flag=true;
                }
            }
            return flag;
        } catch (Exception e) {
            logger.error("Real temperatrue device {} can't get relation in MySQL !", id);
            return flag;
        }  
    }
}
