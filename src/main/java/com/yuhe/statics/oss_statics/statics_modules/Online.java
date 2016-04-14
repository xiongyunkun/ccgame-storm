package com.yuhe.statics.oss_statics.statics_modules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.yuhe.statics.oss_statics.db.ServerDB;
import com.yuhe.statics.oss_statics.db.log.OnlineDB;
import com.yuhe.statics.oss_statics.utils.DateUtils;

import net.sf.json.JSONObject;

/**
 * 统计在线人数
 * 
 * @author xiongyunkun
 *
 */
public class Online extends AbstractStaticsModule {

	@Override
	public boolean execute(List<String> logList) {
		Map<String, List<Map<String, String>>> platformResults = new HashMap<String, List<Map<String, String>>>();
		// logList.add("{\"time\":1457616416,\"num\":\"0\",\"hostid\":\"716\"}");
		Map<String, String> staticsHosts = ServerDB.getStaticsServers();
		for (String log : logList) {
			JSONObject json = JSONObject.fromObject(log);
			if (json != null) {
				String onlineNum = json.getString("num");
				String hostID = json.getString("hostid");
				String time = DateUtils.getFloorTime();
				if (Integer.parseInt(onlineNum) > 0 && staticsHosts.containsKey(hostID)) {
					// 获得该HostID对应的所有平台列表
					List<String> platformList = ServerDB.getPlatformListByHostID(hostID);
					for (String platformID : platformList) {
						Map<String, String> map = new HashMap<String, String>();
						map.put("PlatformID", platformID);
						map.put("HostID", hostID);
						map.put("Time", time);
						map.put("OnlineNum", onlineNum);
						List<Map<String, String>> platformResult = platformResults.get(platformID);
						if (platformResult == null)
							platformResult = new ArrayList<Map<String, String>>();
						platformResult.add(map);
						platformResults.put(platformID, platformResult);
					}
				}
			}
		}
		// 插入数据库
		Iterator<String> it = platformResults.keySet().iterator();
		while (it.hasNext()) {
			String platformID = it.next();
			List<Map<String, String>> platformResult = platformResults.get(platformID);
			OnlineDB db = new OnlineDB();
			db.batchInsert(platformID, platformResult);
		}
		return true;
	}
}
