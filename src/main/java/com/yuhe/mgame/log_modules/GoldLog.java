package com.yuhe.mgame.log_modules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.yuhe.mgame.db.ServerDB;
import com.yuhe.mgame.db.log.CommonDB;
import com.yuhe.mgame.utils.RegUtils;

import net.sf.json.JSONObject;

public class GoldLog extends AbstractLogModule {
	// 需要提取的列，注意要跟数据库中对应，前面的HostID和后面的时间都不要在这里填
	private static final String[] LOG_COLS = { "Uid", "Urs", "Changes", "Reason", "Remain", "Name", "SDKInfo" };
	private static final String[] DB_COLS = { "HostID", "Uid", "Urs", "Changes", "Reason", "Remain", "Name", "Time" };
	private static String TBL_NAME = "tblGoldLog";

	@Override
	public Map<String, List<Map<String, String>>> execute(List<String> logList, Map<String, String> staticsHosts) {
		Map<String, List<Map<String, String>>> platformResults = new HashMap<String, List<Map<String, String>>>();

		for (String logStr : logList) {
			JSONObject json = JSONObject.fromObject(logStr);
			if (json != null) {
				String message = json.getString("message");
				String hostID = json.getString("hostid");
				if (StringUtils.isNotBlank(message) && staticsHosts.containsKey(hostID)) {
					Map<String, String> map = new HashMap<String, String>();
					String type = json.getString("type");
					boolean isSub = type.indexOf("sub") != -1;
					map.put("HostID", hostID);
					String time = RegUtils.getLogTime(message);
					map.put("Time", time);
					String sdkInfo = "";
					for (String col : LOG_COLS) {
						String value = RegUtils.getLogValue(message, col, "");
						if (col.equals("Changes") && isSub) {
							value = "-" + value; // 如果是扣除钻石的话前面添加负号
						}
						map.put(col, value);
						if (col.equals("SDKInfo")) {
							sdkInfo = value;
						}
					}
					String platformID = ServerDB.getPlatformIDBySDKID(sdkInfo);
					List<Map<String, String>> platformResult = platformResults.get(platformID);
					if (platformResult == null)
						platformResult = new ArrayList<Map<String, String>>();
					platformResult.add(map);
					platformResults.put(platformID, platformResult);
				}
			}
		}
		// 插入数据库
		Iterator<String> it = platformResults.keySet().iterator();
		while (it.hasNext()) {
			String platformID = it.next();
			List<Map<String, String>> platformResult = platformResults.get(platformID);
			CommonDB.batchInsertByDate(platformID, platformResult, DB_COLS, TBL_NAME);
		}
		return platformResults;
	}

	@Override
	public String getStaticsIndex() {
		return "Gold";
	}

	@Override
	public Map<String, List<Map<String, String>>> execute4Kafka(JSONObject json, Map<String, String> staticsHosts) {
		Map<String, List<Map<String, String>>> platformResults = new HashMap<String, List<Map<String, String>>>();

		String message = json.getString("message");
		String hostID = json.getString("hostid");
		if (StringUtils.isNotBlank(message) && staticsHosts.containsKey(hostID)) {
			Map<String, String> map = new HashMap<String, String>();
			String type = json.getString("type");
			boolean isSub = type.indexOf("sub") != -1;
			map.put("HostID", hostID);
			String time = RegUtils.getLogTime(message);
			map.put("Time", time);
			String sdkInfo = "";
			for (String col : LOG_COLS) {
				String value = RegUtils.getLogValue(message, col, "");
				if (col.equals("Changes") && isSub) {
					value = "-" + value; // 如果是扣除钻石的话前面添加负号
				}
				map.put(col, value);
				if (col.equals("SDKInfo")) {
					sdkInfo = value;
				}
			}
			String platformID = ServerDB.getPlatformIDBySDKID(sdkInfo);
			List<Map<String, String>> platformResult = platformResults.get(platformID);
			if (platformResult == null)
				platformResult = new ArrayList<Map<String, String>>();
			platformResult.add(map);
			platformResults.put(platformID, platformResult);
		}
		// 插入数据库
		Iterator<String> it = platformResults.keySet().iterator();
		while (it.hasNext()) {
			String platformID = it.next();
			List<Map<String, String>> platformResult = platformResults.get(platformID);
			CommonDB.batchInsertByDate(platformID, platformResult, DB_COLS, TBL_NAME);
		}
		return platformResults;
	}

}
