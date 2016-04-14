package com.yuhe.statics.oss_statics.statics_modules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.yuhe.statics.oss_statics.db.ServerDB;
import com.yuhe.statics.oss_statics.db.log.AbstractDB;
import com.yuhe.statics.oss_statics.db.log.ArenaLogDB;
import com.yuhe.statics.oss_statics.utils.RegUtils;

import net.sf.json.JSONObject;

/**
 * 拉取arena.log相关日志并且入库,相关日志文件有：arena.log
 * 
 * @author xiongyunkun
 *
 */
public class ArenaLog extends AbstractStaticsModule {

	private static final String[] LOG_COLS = { "Uid", "Urs", "Name", "TargetUid", "TargetUrs", "TargetName", "Rank",
			"TRank", "FightGroup", "DefendGroup", "FightName", "DefendName", "SDKInfo", "IsWin" };

	@Override
	public boolean execute(List<String> logList) {
		Map<String, List<Map<String, String>>> platformResults = new HashMap<String, List<Map<String, String>>>();
		Map<String, String> staticsHosts = ServerDB.getStaticsServers();
		for (String logStr : logList) {
			JSONObject json = JSONObject.fromObject(logStr);
			if (json != null) {
				String message = json.getString("message");
				String hostID = json.getString("hostid");
				if (!message.isEmpty() && !message.equals(" ") && staticsHosts.containsKey(hostID)) {
					Map<String, String> map = new HashMap<String, String>();
					
					map.put("HostID", hostID);
					String time = RegUtils.getLogTime(message);
					map.put("Time", time);
					String sdkInfo = "";
					for (String col : LOG_COLS) {
						String value = RegUtils.getLogValue(message, col, "");
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
			AbstractDB db = new ArenaLogDB();
			db.batchInsert(platformID, platformResult);
		}
		return true;
	}

}
