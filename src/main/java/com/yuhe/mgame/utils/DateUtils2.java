package com.yuhe.mgame.utils;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;

public class DateUtils2 {

	private static String DateFormat = "yyyy-MM-dd";
	private static String TimeFormat = "yyyy-MM-dd HH:mm:ss";
	private static String SqlDateFormat = "yyyyMMdd";

	/**
	 * 获得startDate和endDate中间所跨天数的列表
	 * 
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public static List<Date> getDates(String startDate, String endDate) {
		List<Date> dateList = new ArrayList<Date>();
		String[] parsePatterns = { DateFormat };
		try {
			Date dBegin = DateUtils.parseDate(startDate, parsePatterns);
			Date dEnd = DateUtils.parseDate(endDate, parsePatterns);
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
		String[] parsePatterns = { TimeFormat };
		String dateStr = DateFormatUtils.format(new Date(), SqlDateFormat);
		try {
			Date date = DateUtils.parseDate(time, parsePatterns);
			dateStr = DateFormatUtils.format(date, SqlDateFormat);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return dateStr;
	}

	/**
	 * 获得5分钟基准时间，例如现在时间是20:09分，那么5分钟基准时间是20:05分
	 * 
	 * @return timeStr
	 */
	public static String getFloorTime(int timestamp) {
		// 先获得当天0点的时间戳
		Calendar benCal = Calendar.getInstance();
		benCal.set(Calendar.HOUR_OF_DAY, 0);
		benCal.set(Calendar.SECOND, 0);
		benCal.set(Calendar.MINUTE, 0);
		benCal.set(Calendar.MILLISECOND, 0);
		long benTime = benCal.getTimeInMillis();
		// 当前时间戳
		Calendar cal = Calendar.getInstance();
		long nowTime = cal.getTimeInMillis();
		if(timestamp != -1){
			nowTime = (long)timestamp * 1000;
		}
		long diff = nowTime - benTime;
		long floor = Math.floorDiv(diff, 300000);
		long floorTime = benTime + floor * 300000;
		String timeStr = DateFormatUtils.format(floorTime, TimeFormat);
		return timeStr;
	}

	/**
	 * 获得dateStr相隔overdays的日期
	 * 
	 * @param dateStr
	 * @param overdays
	 * @return
	 */
	public static String getOverDate(String dateStr, int overdays) {
		String resultDay = null;
		String[] parsePatterns = { DateFormat };
		try {
			Date date = DateUtils.parseDate(dateStr, parsePatterns);
			date = DateUtils.addDays(date, overdays);
			resultDay = DateFormatUtils.format(date, DateFormat);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return resultDay;
	}
	/**
	 * 根据时间戳返回时间格式yyyy-MM-dd HH:mm:ss
	 * @param timestamp
	 * @return
	 */
	public static String getTimeStr(int timestamp){
		String timeStr = DateFormatUtils.format(new Date(timestamp * 1000L), TimeFormat);
		return timeStr;
	}
	
	/**
	 * 根据时间格式返回时间戳,时间格式为:yyyy-MM-dd HH:mm:ss
	 * @param timeStr
	 * @return
	 */
	public static int GetTimestamp(String timeStr){
		String[] parsePatterns = { TimeFormat };
		Calendar cal = Calendar.getInstance();
		long nowTime = cal.getTimeInMillis();
		try {
			Date date = DateUtils.parseDate(timeStr, parsePatterns);
			nowTime = date.getTime();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		int timestamp = (int)(nowTime/1000);
		return timestamp;
	}
	
	public static void main(String[] args){
		String time = "2016-06-28 15:25:00";
		System.out.println(GetTimestamp(time));
	}
}
