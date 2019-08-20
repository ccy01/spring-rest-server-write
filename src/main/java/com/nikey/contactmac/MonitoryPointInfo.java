package com.nikey.contactmac;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitoryPointInfo {
    
    private static final Logger logger = LoggerFactory.getLogger(MonitoryPointInfo.class);
    
    private static final MonitoryPointInfo instance = new MonitoryPointInfo();
    public static MonitoryPointInfo getInstance() {
        return instance;
    }

    public String[] pointInfo (String mCode) {
        
        if (mCode.equals("2001")) {
            String companyId = "2";
            String contactMAC = "90180035610100";
            return new String[] {companyId, contactMAC};
        }
        if (mCode.equals("3001")) {
            String companyId = "3";
            String contactMAC = "90180007610100";
            return new String[] {companyId, contactMAC};
        }
        if (mCode.equals("4001")) {
            String companyId = "4";
            String contactMAC = "90180005410100";
            return new String[] {companyId, contactMAC};
        }
        if (mCode.equals("4002")) {
            String companyId = "4";
            String contactMAC = "90180006780100";
            return new String[] {companyId, contactMAC};
        }
        if (mCode.equals("5001")) {
            String companyId = "5";
            String contactMAC = "90180010110100";
            return new String[] {companyId, contactMAC};
        }
        if (mCode.equals("5002")) {
            String companyId = "5";
            String contactMAC = "90180011560100";
            return new String[] {companyId, contactMAC};
        }
        if (mCode.equals("5003")) {
            String companyId = "5";
            String contactMAC = "90180013740100";
            return new String[] {companyId, contactMAC};
        }
        if (mCode.equals("5004")) {
            String companyId = "5";
            String contactMAC = "90180014170100";
            return new String[] {companyId, contactMAC};
        }
        if (mCode.equals("5005")) {
            String companyId = "5";
            String contactMAC = "90180016810100";
            return new String[] {companyId, contactMAC};
        }
        if (mCode.equals("5006")) {
            String companyId = "5";
            String contactMAC = "90180017820100";
            return new String[] {companyId, contactMAC};
        }
        if (mCode.equals("5007")) {
            String companyId = "5";
            String contactMAC = "90180018140100";
            return new String[] {companyId, contactMAC};
        }
        if (mCode.equals("5008")) {
            String companyId = "5";
            String contactMAC = "90180019410100";
            return new String[] {companyId, contactMAC};
        }
        if (mCode.equals("5009")) {
            String companyId = "5";
            String contactMAC = "90180009460100";
            return new String[] {companyId, contactMAC};
        }
        if (mCode.equals("5010")) {
            String companyId = "5";
            String contactMAC = "90180015440100";
            return new String[] {companyId, contactMAC};
        }
        if (mCode.equals("5011")) {
            String companyId = "5";
            String contactMAC = "90180008010100";
            return new String[] {companyId, contactMAC};
        } 
        if (mCode.equals("6001")) {
            String companyId = "6";
            String contactMAC = "90180023860100";
            return new String[] {companyId, contactMAC};
        } 
        if (mCode.equals("6002")) {
            String companyId = "6";
            String contactMAC = "90180025560100";
            return new String[] {companyId, contactMAC};
        } 
        if (mCode.equals("6003")) {
            String companyId = "6";
            String contactMAC = "90180022030100";
            return new String[] {companyId, contactMAC};
        } 
        if (mCode.equals("6004")) {
            String companyId = "6";
            String contactMAC = "90180021680100";
            return new String[] {companyId, contactMAC};
        } 
        if (mCode.equals("6005")) {
            String companyId = "6";
            String contactMAC = "90180024290100";
            return new String[] {companyId, contactMAC};
        } 
       logger.error("没有找到这个deviceID");
       return null;
    }  
}
