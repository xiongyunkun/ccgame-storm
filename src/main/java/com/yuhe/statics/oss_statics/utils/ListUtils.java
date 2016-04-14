package com.yuhe.statics.oss_statics.utils;

import java.util.List;

public class ListUtils {
	/**
	 * 将数组用seperator分隔符拼接成字符串
	 * @param list
	 * @param seperator:分隔符
	 * @return
	 */
	public static String list2String(List<String> list, String seperator){
		StringBuilder sb = new StringBuilder();  
		for (int i = 0; i < list.size(); i++) {  
		    sb.append(list.get(i)).append(seperator);
		}  
		return sb.toString().substring(0,sb.toString().length()-seperator.length());
	}
}
