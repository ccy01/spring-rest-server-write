package com.nikey.service;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.nikey.mapper.TransFormerInfoMapper;
import com.nikey.util.ServiceHelper;

@Service
public class TransFormerInfoService
{
    
    /**
     * slfj
     */
    Logger logger = LoggerFactory.getLogger(getClass());
    
    public TransFormerInfoService() {
    	ServiceHelper.instance().setTransFormerInfoService(this);
	}
    
    @Autowired
    private TransFormerInfoMapper transFormerInfoMapper;
    
    private Map<Short, String> cache = new HashMap<Short, String>();
    
    public String isTransFormerById(Short id) {
        synchronized (cache) {
            if(cache.get(id) != null) {
                return cache.get(id);
            }
        }
        try {
            String result = transFormerInfoMapper.isTransFormerById(id);
            if(result != null) {
                synchronized (cache) {
                    cache.put(id, result);
                }
            }
            return result;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    

}
