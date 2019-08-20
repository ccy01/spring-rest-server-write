package com.nikey.bean;

/**
 * 汉光监测点上线
 *
 * @author JayzeeZhang
 * @date 2018年3月20日
 */
public class HokoPointinfo {

    private String companyForShort; //公司简称
    private String deviceName; //设备名称
    private String prjCode;
    private String magName;
    private String mcode;
    private Integer companyId;
    private Integer deviceId;
    private Integer groupId;

    private Integer powerId; // 电源ID
    private Float maxDemand; // 合同最大需量

    private Float maxReal; // 实际最大需量
    private Long maxRealTime; // 实际最大需量发生时间

    private Float faultPercent; // 最大需量报警阈值（%）
    private Float warnPercent; // 最大需量预警阈值（%）

    private AlarmCounter dayWarnCounter; // 预警计数器
    private AlarmCounter monthWarnCounter; // 预警计数器
    private AlarmCounter dayFaultCounter; // 报警计数器
    private AlarmCounter monthFaultCounter; // 报警计数器

    public AlarmCounter getDayWarnCounter() {
        return dayWarnCounter;
    }

    public void setDayWarnCounter(AlarmCounter dayWarnCounter) {
        this.dayWarnCounter = dayWarnCounter;
    }

    public AlarmCounter getMonthWarnCounter() {
        return monthWarnCounter;
    }

    public void setMonthWarnCounter(AlarmCounter monthWarnCounter) {
        this.monthWarnCounter = monthWarnCounter;
    }

    public AlarmCounter getDayFaultCounter() {
        return dayFaultCounter;
    }

    public void setDayFaultCounter(AlarmCounter dayFaultCounter) {
        this.dayFaultCounter = dayFaultCounter;
    }

    public AlarmCounter getMonthFaultCounter() {
        return monthFaultCounter;
    }

    public void setMonthFaultCounter(AlarmCounter monthFaultCounter) {
        this.monthFaultCounter = monthFaultCounter;
    }

    public void setHoko(String prjCode, String magName) {
        this.prjCode = prjCode;
        this.magName = magName;
    }

    public String getPrjCode() {
        return prjCode;
    }

    public void setPrjCode(String prjCode) {
        this.prjCode = prjCode;
    }

    public String getMagName() {
        return magName;
    }

    public void setMagName(String magName) {
        this.magName = magName;
    }

    public String getMcode() {
        return mcode;
    }

    public void setMcode(String mcode) {
        this.mcode = mcode;
    }

    public Integer getCompanyId() {
        return companyId;
    }

    public void setCompanyId(Integer companyId) {
        this.companyId = companyId;
    }

    public Integer getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(Integer deviceId) {
        this.deviceId = deviceId;
    }

    public Integer getGroupId() {
        return groupId;
    }

    public void setGroupId(Integer groupId) {
        this.groupId = groupId;
    }

    public Integer getPowerId() {
        return powerId;
    }

    public void setPowerId(Integer powerId) {
        this.powerId = powerId;
    }

    public Float getMaxDemandContract() {
        return maxDemand;
    }

    public void setMaxDemandContract(Float maxDemand) {
        this.maxDemand = maxDemand;
    }

    public Float getMaxDemandReal() {
        return maxReal;
    }

    public void setMaxDemandReal(Float maxReal) {
        this.maxReal = maxReal;
    }

    public Long getMaxDemandRealTime() {
        return maxRealTime;
    }

    public void setMaxDemandRealTime(Long maxRealTime) {
        this.maxRealTime = maxRealTime;
    }

    public Float getFaultPercent() {
        return faultPercent;
    }

    public void setFaultPercent(Float faultPercent) {
        this.faultPercent = faultPercent;
    }

    public Float getWarnPercent() {
        return warnPercent;
    }

    public void setWarnPercent(Float warnPercent) {
        this.warnPercent = warnPercent;
    }

    public String getCompanyForShort() {
        return companyForShort;
    }

    public void setCompanyForShort(String companyForShort) {
        this.companyForShort = companyForShort;
    }

    public String getDeviceName() {
        return deviceName;
    }

    public void setDeviceName(String deviceName) {
        this.deviceName = deviceName;
    }


}
