package com.yuhe.mgame.utils;

import java.util.HashMap;
import java.util.Map;

public class FuncUtils {
	@SuppressWarnings("rawtypes")
	public static Map getOrInit(Map<String, Map> map, String key){
		Map value = map.get(key);
		if(value == null){
			value = new HashMap();
			map.put(key, value);
		}
		return value;
	}
}
