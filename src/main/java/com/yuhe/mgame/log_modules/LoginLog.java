package com.yuhe.mgame.log_modules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.yuhe.mgame.db.ServerDB;
import com.yuhe.mgame.db.UserInfoDB;
import com.yuhe.mgame.db.log.CommonDB;
import com.yuhe.mgame.utils.RegUtils;

import net.sf.json.JSONObject;

/**
 * 解析登陆日志
 * 
 * @author xiongyunkun
 *
 */
public class LoginLog extends AbstractLogModule {
	// 需要提取的列，注意要跟数据库中对应，前面的HostID和后面的时间都不要在这里填
	private static final String[] LOG_COLS = { "Uid", "Name", "Urs", "Level", "Ip", "PhoneInfo", "SDKInfo", "HeadId" };
	private static final String[] DB_COLS = { "HostID", "Uid", "Name", "Urs", "Level", "Ip", "PhoneInfo", "Time" };
	private static String TBL_NAME = "tblLoginLog";

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
					map.put("HostID", hostID);
					map.put("SrcHostID", hostID);
					String time = RegUtils.getLogTime(message);
					map.put("Time", time);
					String sdkInfo = "";
					for (String col : LOG_COLS) {
						String value = RegUtils.getLogValue(message, col, "");
						map.put(col, value);
						if (col.equals("SDKInfo")) {
							sdkInfo = value;
						}
						// 针对台湾地区还需要统计国家，其他情况不需要统计这个注释掉
						if (col.equals("PhoneInfo")) {
							String country = RegUtils.getCountry(value);
							map.put("Country", country);
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
			UserInfoDB.batchInsert(platformID, platformResult);
		}
		return platformResults;
	}

	@Override
	public String getStaticsIndex() {
//		return "MixLoginStatics"; // 统计留存等信息
		return null;
	}

	@Override
	public Map<String, List<Map<String, String>>> execute4Kafka(JSONObject json, Map<String, String> staticsHosts) {
		Map<String, List<Map<String, String>>> platformResults = new HashMap<String, List<Map<String, String>>>();

		String message = json.getString("message");
		String hostID = json.getString("hostid");
		if (StringUtils.isNotBlank(message) && staticsHosts.containsKey(hostID)) {
			Map<String, String> map = new HashMap<String, String>();
			map.put("HostID", hostID);
			map.put("SrcHostID", hostID);
			String time = RegUtils.getLogTime(message);
			map.put("Time", time);
			String sdkInfo = "";
			for (String col : LOG_COLS) {
				String value = RegUtils.getLogValue(message, col, "");
				map.put(col, value);
				if (col.equals("SDKInfo")) {
					sdkInfo = value;
				}
				// 针对台湾地区还需要统计国家，其他情况不需要统计这个注释掉
				if (col.equals("PhoneInfo")) {
					String country = RegUtils.getCountry(value);
					map.put("Country", country);
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
			UserInfoDB.batchInsert(platformID, platformResult);
		}
		return platformResults;
	}

}
