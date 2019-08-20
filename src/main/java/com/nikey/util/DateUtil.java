package com.nikey.util;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * @author jayzee
 * @date 15 Apr, 2014
 * @description
 */
public class DateUtil {
	
	/**
	 * @param date
	 * @return format string
	 */
	public static String formatToYYMMDD(Date date) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		if(date != null) return format.format(date);
		else return null;
	}
	public static String formatToYYMMDD(Long date) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		if(date != null) return format.format(new Date(date));
		else return null;
	}
	
	/**
	 * @param date
	 * @return parse string
	 */
	public static Date parseYYMMDDToDate(String date) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		if(date != null)
			try {
				return format.parse(date);
			} catch (ParseException e) {
				return null;
			}
		else return null;
	}
	
	/**
	 * @param date
	 * @return format string
	 */
	public static String formatToHHMMSS(Date date) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		if(date != null) return format.format(date);
		else return null;
	}
	public static String formatToHHMMSS(Long date) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		if(date != null) return format.format(new Date(date));
		else return null;
	}
	
	/**
	 * 返回一个Date数据类型
	 * @param date
	 * @return parse string
	 */
	public static Date parseHHMMSSToDate(String date) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		if(date != null)
			try {
				return format.parse(date); 
			} catch (ParseException e) {
				return null;
			}
		else return null;
	}
	
	/**
	 * transfer  the parameter Date to the String (format  "yyyy-MM-dd 00:00:00")
	 * if fail return null,else return String
	 * @param date
	 * @return
	 */
	public static String formatTo000000(Date date)
	{
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd 00:00:00");
		if(date != null) return format.format(date);
		else return null;
	}
	public static String formatTo000000(Long date)
	{
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd 00:00:00");
		if(date != null) return format.format(new Date(date));
		else return null;
	}
	
	public static String getThisMoment() {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd_HH_mm_ss_SSS");
		return format.format(new Date());
	}
	
	/**
	 * 获取当天某个整点的时间戳，如今日凌晨0点或1点
	 * @param int 整点
	 * @return long 当天某个整点时间戳
	 */
	public static long getHourTimesOfDay(int hour) {
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.HOUR_OF_DAY, hour);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		return calendar.getTimeInMillis();
	}

	/**
     * 输入为毫秒值，返回hbase行度的timestamp标识
     * @param insertTime
     * @return timestamp
     * 1：整小时
     * 2：整天
     * 3：整月
     * 4：整年
     * 0：其他情况
     */
	public static long getTimestampByInsertTime(long insertTime) {
        String date = formatToHHMMSS(insertTime);
        if(date.contains("01-01 00:00:00")) {
            return 4;
        }
        if(date.contains("01 00:00:00")) {
            return 3;
        }
        if(date.contains("00:00:00")) {
            return 2;
        }
        if(date.contains("00:00")) {
            return 1;
        }
        return 0;
    }
	
	/*----以下为新添方法----*/
	
	/**
     * 获取明天的开始时间
     * @return 默认格式 Wed May 31 14:47:18 CST 2017
     */
    public static Date getBeginDayOfTomorrow() {
        Calendar cal = new GregorianCalendar();
        cal.setTime(getDayBegin());
        cal.add(Calendar.DAY_OF_MONTH, 1);

        return cal.getTime();
    }
    
    /**
     * 获取当天的开始时间
     * @return yyyy-MM-dd HH:mm:ss  格式
     */
    public static java.util.Date getDayBegin() {
        /*
        Calendar cal = new GregorianCalendar();
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();*/
        Date date = new Date();
        return getDayStartTime(date);
    }
    
    /**
     * 获取某个日期的开始时间
     * @param d
     * @return yyyy-MM-dd HH:mm:ss  格式
     */
    public static Timestamp getDayStartTime(Date d) {
        Calendar calendar = Calendar.getInstance();
        if(null != d) calendar.setTime(d);
        calendar.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH),    calendar.get(Calendar.DAY_OF_MONTH), 0, 0, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return new Timestamp(calendar.getTimeInMillis());
    }
	/**
	 * return true stands for the same day
	 * 判断是否同一天，如果是就返回true
	 * 
	 * @param insertTime
	 * @param newValue
	 * @return
	 */
	public static boolean isSameDay(long insertTime, Long newValue) {
		Calendar cal1 = Calendar.getInstance();
		cal1.setTimeInMillis(insertTime);
		
		Calendar cal2 = Calendar.getInstance();
		cal2.setTimeInMillis(newValue);
		
		return (cal1.get(Calendar.DAY_OF_MONTH) == cal2.get(Calendar.DAY_OF_MONTH)) &&
				(cal1.get(Calendar.MONTH) == cal2.get(Calendar.MONTH)) && 
				(cal1.get(Calendar.YEAR) == cal2.get(Calendar.YEAR));
	}
	
	/**
	 * return true stands for is hour time
	 * 
	 * @param insertTime
	 * @param hOUR_SUB
	 * @return
	 */
	public static boolean isHour(long insertTime, Long hOUR_SUB) {
		Calendar cal1 = Calendar.getInstance();
		cal1.setTimeInMillis(insertTime);
		
		long pass = cal1.get(Calendar.MINUTE) * 60 * 1000l + cal1.get(Calendar.SECOND) * 1000l; // 过整点的毫秒值
		return pass < hOUR_SUB;
	}
	
	/**
	 * return true stands for is morning time, 00:00
	 * 
	 * @param insertTime
	 * @param hOUR_SUB
	 * @return
	 */
	public static boolean isMorning(long insertTime, Long hOUR_SUB) {
		Calendar cal1 = Calendar.getInstance();
		cal1.setTimeInMillis(insertTime);
		
		return isHour(insertTime, hOUR_SUB) && cal1.get(Calendar.HOUR_OF_DAY) == 0;
	}
	
	/**
	 * 0 stands for 00:00, 1 stands fro 01:00, and so on
	 * 
	 * @param insertTime
	 * @return
	 */
	public static int getHourIndex(long insertTime) {
		Calendar cal1 = Calendar.getInstance();
		cal1.setTimeInMillis(insertTime);
		return cal1.get(Calendar.HOUR_OF_DAY);
	}
	
	public static String getBeginDayOfThisMonth() {
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.DAY_OF_MONTH, 1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		return formatToHHMMSS(cal.getTimeInMillis());
	}
	
	public static String getBeginDayOfNextMonth() {
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.MONTH, cal.get(Calendar.MONTH) + 1);
		cal.set(Calendar.DAY_OF_MONTH, 1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		return formatToHHMMSS(cal.getTimeInMillis());
	}
	
	public static boolean isSameMonth(Long maxDemandRealTime, long currentTimeMillis) {
		Calendar cal1 = Calendar.getInstance();
		cal1.setTimeInMillis(maxDemandRealTime);
		
		Calendar cal2 = Calendar.getInstance();
		cal2.setTimeInMillis(currentTimeMillis);
		
		return cal1.get(Calendar.YEAR) == cal2.get(Calendar.YEAR) &&
				cal1.get(Calendar.MONTH) == cal2.get(Calendar.MONTH);
	}
	/**
	 * 验证是否是"yyyy/MM/dd HH:mm:ss"格式
	 * @param str 日期
	 * @return 
	 */
	public static boolean isValidDate(String str) {
        boolean convertSuccess = true;
        // 指定日期格式为四位年/两位月份/两位日期，注意yyyy/MM/dd区分大小写；
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            // 设置lenient为false.
            // 否则SimpleDateFormat会比较宽松地验证日期，比如2007/02/29会被接受，并转换成2007/03/01
            format.setLenient(false);
            format.parse(str);
        } catch (ParseException e) {
            // e.printStackTrace();
            // 如果throw java.text.ParseException或者NullPointerException，就说明格式不对
            convertSuccess = false;
        }
        return convertSuccess;
    }
	
	
	public static void main(String[] args) {
		System.out.println(isSameMonth(System.currentTimeMillis(), System.currentTimeMillis()));
	}
	
}
