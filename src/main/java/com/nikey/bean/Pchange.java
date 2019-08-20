package com.nikey.bean;

import com.nikey.util.DateUtil;

/**
 * @author Jayzee
 * @date 2017年3月8日 下午6:45:17
 */
public class Pchange {
    
    private Integer id;
    private Integer device_id;
    private String device_name;
    private Float ctr;
    private Float ptr;
    private Float standardU;
    private Integer is_phase_voltage;
    private String install_addr;
    private Long insert_time;
    private String dateString;
    
    public Integer getId() {
        return id;
    }
    public void setId(Integer id) {
        this.id = id;
    }
    public Integer getDevice_id() {
        return device_id;
    }
    public void setDevice_id(Integer device_id) {
        this.device_id = device_id;
    }
    public String getDevice_name() {
        return device_name;
    }
    public void setDevice_name(String device_name) {
        this.device_name = device_name;
    }
    public Float getCtr() {
        return ctr;
    }
    public void setCtr(Float ctr) {
        this.ctr = ctr;
    }
    public Float getPtr() {
        return ptr;
    }
    public void setPtr(Float ptr) {
        this.ptr = ptr;
    }
    public Float getStandardU() {
        return standardU;
    }
    public void setStandardU(Float standardU) {
        this.standardU = standardU;
    }
    public Integer getIs_phase_voltage() {
        return is_phase_voltage;
    }
    public void setIs_phase_voltage(Integer is_phase_voltage) {
        this.is_phase_voltage = is_phase_voltage;
    }
    public String getInstall_addr() {
        return install_addr;
    }
    public void setInstall_addr(String install_addr) {
        this.install_addr = install_addr;
    }
    public Long getInsert_time() {
        return insert_time;
    }
    public void setInsert_time(Long insert_time) {
        this.insert_time = insert_time;
    }
    public String getDateString() {
        return dateString;
    }
    public void setDateString() {
        this.dateString = DateUtil.formatToHHMMSS(this.insert_time);
    }
}
