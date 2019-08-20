package com.nikey.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.nikey.mapper.GroupInfoMapper;
import com.nikey.util.ServiceHelper;

@Service
public class GroupInfoService
{
    Logger logger = LoggerFactory.getLogger(getClass());
    
    public GroupInfoService() {
    	ServiceHelper.instance().setGroupInfoService(this);
	}
    
    @Autowired
    GroupInfoMapper groupMapper;
    
    private Map<Short,Short>cache=new HashMap<Short,Short>();
    
    private Map<Short,Short>companyIdAndGroudIdCache=new HashMap<Short,Short>();

    public short getCompanyIdByGroudId(short groudId){
        synchronized (companyIdAndGroudIdCache)
        {
            if(companyIdAndGroudIdCache.get(groudId)!=null){
                return companyIdAndGroudIdCache.get(groudId);
            }
            try
            {
                short companyId=groupMapper.getCompanyIdByGroupId(groudId);
                synchronized (companyIdAndGroudIdCache)
                {
                    companyIdAndGroudIdCache.put(groudId, companyId);
                }
                return companyId;
            } catch (Exception e)
            {
                e.printStackTrace();
                logger.error(groudId+":This groupId no match companyId");
                return 0;
            }
        }
    }
    
    public short getGroupCompanyInfo(short companyId){
        synchronized (cache)
        {
            if(cache.get(companyId)!=null){
                return cache.get(companyId);
            }
            try
            {
                short groupId=groupMapper.getGroupCompanyInfo(companyId);
                synchronized (cache)
                {
                    cache.put(companyId, groupId);
                }
                return groupId;
            } catch (Exception e)
            {
                logger.error("This {} no company groupId", companyId);
                return 0;
            }
        }
    }

    public int insertPchage(List<Object> list) {
        return groupMapper.insertPchage(list);
    }
    
}
