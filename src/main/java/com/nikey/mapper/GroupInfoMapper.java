package com.nikey.mapper;

import java.util.List;

public interface GroupInfoMapper
{
    short getGroupCompanyInfo(short companyId);
    short getCompanyIdByGroupId(short groudId);
    int insertPchage(List<Object> list);
}
