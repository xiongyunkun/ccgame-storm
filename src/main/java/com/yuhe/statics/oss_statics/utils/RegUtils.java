package com.yuhe.statics.oss_statics.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegUtils {
	
	/**
	 * 从日志中提取对应字段
	 * @param content
	 * @param key
	 * @param defaultValue
	 * @return 
	 */
	public static String getLogValue(String logLine, String key, String defaultValue){
		String value = defaultValue;
		String pStr = "[),]"+ key + "=(.*?)(,|$)";
		Pattern p = Pattern.compile(pStr);
		Matcher m = p.matcher(logLine);
		while(m.find()){
			value = m.group(1);
		}
		return value;
	}
	/**
	 * 从日志中提取时间
	 * @param logLine
	 * @return
	 */
	public static String getLogTime(String logLine){
		String pStr = "^\\[(.*?)\\]";
		Pattern p = Pattern.compile(pStr);
		Matcher m = p.matcher(logLine);
		String time = "";
		while(m.find()){
			time = m.group(1);
		}
		return time;
	}
	
	

}
