package com.nikey.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 
 * @author jtb
 *
 */
public class TimeConvertUtil {

    /**
     * 将时间戳转换成年月日时分秒
     * 
     * @param 
     * @return
     */
    public static String stampToDate(String time){
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long lt = new Long(time);
        Date date = new Date(lt);
        res = simpleDateFormat.format(date);
        return res;
    }	
    
    

    /**
     * 将时间转换为时间戳
     *    
     * @param s
     * @return
     * @throws ParseException
     */
    public static String dateToStamp(String s) throws ParseException {
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = simpleDateFormat.parse(s);
        long ts = date.getTime();
        res = String.valueOf(ts);
        return res;
    }
	
}
