package com.yuhe.statics.oss_statics.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class DateUtils {
	/**
	 * 获得startDate和endDate中间所跨天数的列表
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public static List<Date> getDates(String startDate, String endDate){
		List<Date> dateList = new ArrayList<Date>();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		try {
			Date dBegin = sdf.parse(startDate);
			Date dEnd = sdf.parse(endDate);
	        dateList.add(dBegin);
	        Calendar cal = Calendar.getInstance();  
	        cal.setTime(dBegin);  
	        boolean bContinue = true;  
	        while (bContinue) {  
	            // 根据日历的规则，为给定的日历字段添加或减去指定的时间量  
	            cal.add(Calendar.DAY_OF_MONTH, 1);  
	            // 测试此日期是否在指定日期之后  
	            if (dEnd.after(cal.getTime())) {  
	            	dateList.add(cal.getTime());  
	            } else {  
	                break;  
	            }  
	        } 
	        dateList.add(dEnd);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return dateList;
	}
	/**
	 * 从时间中提取数据库分表对应的日期
	 * @param time
	 * @return dateStr: 格式为yyyyMMdd
	 */
	public static String getSqlDate(String time){
		SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
		String dateStr = dateFormat.format(new Date());
		try {
			dateStr = dateFormat.format(timeFormat.parse(time));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return dateStr;
	}
	/**
	 * 获得5分钟基准时间，例如现在时间是20:09分，那么5分钟基准时间是20:05分
	 * @return timeStr
	 */
	public static String getFloorTime(){
		//先获得当天0点的时间戳
		Calendar benCal = Calendar.getInstance();
		benCal.set(Calendar.HOUR_OF_DAY, 0);
		benCal.set(Calendar.SECOND, 0);
		benCal.set(Calendar.MINUTE, 0);
		benCal.set(Calendar.MILLISECOND, 0);
		long benTime = benCal.getTimeInMillis();
		//当前时间戳
		Calendar cal = Calendar.getInstance();
		long nowTime = cal.getTimeInMillis();
		long diff = nowTime - benTime;
		long floor = Math.floorDiv(diff, 300000);
		long floorTime = benTime + floor * 300000;
		SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String timeStr = timeFormat.format(floorTime);
		return timeStr;
	}
}
