package com.yuhe.mgame.log_modules;

import java.util.List;
import java.util.Map;

import net.sf.json.JSONObject;

public abstract class AbstractLogModule {
	/**
	 * 供redis调用
	 * @param logList
	 * @param hostMap
	 * @return
	 */
	public abstract Map<String, List<Map<String, String>>> execute(List<String> logList, Map<String,String> hostMap);
	public abstract String getStaticsIndex();
	/**
	 * 供kafka调用
	 * @param json
	 * @param staticsHosts
	 * @return
	 */
	public abstract Map<String, List<Map<String, String>>> execute4Kafka(JSONObject json, Map<String,String> staticsHosts);

}
