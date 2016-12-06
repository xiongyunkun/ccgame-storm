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
import com.yuhe.mgame.db.statics.AddPlayerDB;
import com.yuhe.mgame.utils.DateUtils2;
import com.yuhe.mgame.utils.RegUtils;

import net.sf.json.JSONObject;

/**
 * 拉取addplayer.log相关日志并且入库,相关日志文件有：addplayer.log
 * 
 * @author xiongyunkun
 *
 */
public class AddPlayerLog extends AbstractLogModule {

	private static final String[] LOG_COLS = { "Uid", "Urs", "Sex", "PhoneInfo", "SDKInfo", "Name" };
	private static final String[] DB_COLS = { "HostID", "Uid", "Urs", "Sex", "PhoneInfo", "Name", "Time" };
	private static String TBL_NAME = "tblAddPlayerLog";

	@Override
	public Map<String, List<Map<String, String>>> execute(List<String> logList, Map<String, String> staticsHosts) {
		Map<String, List<Map<String, String>>> platformResults = new HashMap<String, List<Map<String, String>>>();
		Map<String, Map<String, Integer>> platformNums = new HashMap<String, Map<String, Integer>>();
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
					// 同时还要更新platformNums
					Map<String, Integer> hostMap = platformNums.getOrDefault(platformID,
							new HashMap<String, Integer>());
					int num = hostMap.getOrDefault(hostID, 0);
					hostMap.put(hostID, num + 1);
					platformNums.put(platformID, hostMap);
				}
			}
		}
		// 插入数据库
		if (platformResults.size() > 0) {
			Iterator<String> it = platformResults.keySet().iterator();
			while (it.hasNext()) {
				String platformID = it.next();
				List<Map<String, String>> platformResult = platformResults.get(platformID);
				CommonDB.batchInsertByDate(platformID, platformResult, DB_COLS, TBL_NAME);
				UserInfoDB.batchUpdateRegTime(platformID, platformResult);
			}
			// 5分钟实时注册
			Update5MinNum(platformNums);
		}
		return platformResults;
	}

	/**
	 * 更新5分钟实时注册人数
	 * 
	 * @param platformNums
	 * @return
	 */
	private boolean Update5MinNum(Map<String, Map<String, Integer>> platformNums) {
		Iterator<String> it = platformNums.keySet().iterator();
		String floorTime = DateUtils2.getFloorTime(-1);
		AddPlayerDB db = new AddPlayerDB();
		while (it.hasNext()) {
			String platformID = it.next();
			Map<String, Integer> hostMap = platformNums.get(platformID);
			Iterator<String> hIt = hostMap.keySet().iterator();
			while (hIt.hasNext()) {
				Map<String, String> numResults = new HashMap<String, String>();
				String hostID = hIt.next();
				int num = hostMap.get(hostID);
				numResults.put("PlatformID", platformID);
				numResults.put("HostID", hostID);
				numResults.put("RegNum", Integer.toString(num));
				numResults.put("Time", floorTime);
				db.update5MinNum(platformID, numResults);
			}
		}
		return true;
	}

	@Override
	public String getStaticsIndex() {
		return "HistoryReg";
	}

	@Override
	public Map<String, List<Map<String, String>>> execute4Kafka(JSONObject json, Map<String, String> staticsHosts) {
		Map<String, List<Map<String, String>>> platformResults = new HashMap<String, List<Map<String, String>>>();
		Map<String, Map<String, Integer>> platformNums = new HashMap<String, Map<String, Integer>>();

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
			// 同时还要更新platformNums
			Map<String, Integer> hostMap = platformNums.getOrDefault(platformID, new HashMap<String, Integer>());
			int num = hostMap.getOrDefault(hostID, 0);
			hostMap.put(hostID, num + 1);
			platformNums.put(platformID, hostMap);
		}
		// 插入数据库
		if (platformResults.size() > 0) {
			Iterator<String> it = platformResults.keySet().iterator();
			while (it.hasNext()) {
				String platformID = it.next();
				List<Map<String, String>> platformResult = platformResults.get(platformID);
				CommonDB.batchInsertByDate(platformID, platformResult, DB_COLS, TBL_NAME);
				UserInfoDB.batchUpdateRegTime(platformID, platformResult);
			}
			// 5分钟实时注册
			Update5MinNum(platformNums);
		}
		return platformResults;
	}

}
