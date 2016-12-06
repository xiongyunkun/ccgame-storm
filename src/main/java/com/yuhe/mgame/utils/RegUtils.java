package com.yuhe.mgame.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

public class RegUtils {

	/**
	 * 从日志中提取对应字段
	 * 
	 * @param content
	 * @param key
	 * @param defaultValue
	 * @return
	 */
	public static String getLogValue(String logLine, String key, String defaultValue) {
		String value = defaultValue;
		String pStr = "[),]" + key + "=(.*?)(,|$)";
		Pattern p = Pattern.compile(pStr);
		Matcher m = p.matcher(logLine);
		while (m.find()) {
			value = m.group(1);
		}
		return value;
	}

	/**
	 * 从日志中提取时间
	 * 
	 * @param logLine
	 * @return
	 */
	public static String getLogTime(String logLine) {
		String pStr = "^\\[(.*?)\\]";
		Pattern p = Pattern.compile(pStr);
		Matcher m = p.matcher(logLine);
		String time = "";
		while (m.find()) {
			time = m.group(1);
		}
		return time;
	}

	/**
	 * 从PhoneInfo字段中获得国家标示，分号分割第九个字段
	 * 
	 * @param phoneInfo
	 * @return
	 */
	public static String getCountry(String phoneInfo) {
		String country = "TW";
		String[] strs = StringUtils.splitPreserveAllTokens(phoneInfo, ";");
		if (strs.length >= 10) {
			country = strs[9];
		}
		return country;
	}
	/**
	 * 从PhoneInfo字段中获取IEMI设备号，分号分割第8个字段
	 * @param phoneInfo
	 * @return
	 */
	public static String getIMEI(String phoneInfo){
		String imei = "";
		String[] strs = StringUtils.splitPreserveAllTokens(phoneInfo, ";");
		if (strs.length >= 10) {
			imei = strs[7];
		}
		return imei;
	}

}
