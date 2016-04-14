package com.yuhe.statics.oss_statics.statics_modules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.yuhe.statics.oss_statics.db.ServerDB;
import com.yuhe.statics.oss_statics.db.log.AbstractDB;
import com.yuhe.statics.oss_statics.db.log.ClientLoadLogDB;
import com.yuhe.statics.oss_statics.utils.RegUtils;

import net.sf.json.JSONObject;

public class W3Log extends AbstractStaticsModule {

	private static final String[] LOG_COLS = { "IMEI", "Operate", "PhoneInfo" };
	private static final Map<String, String> STEP_MAP = new HashMap<String, String>(){
		private static final long serialVersionUID = 1L;
		{
			put("Open", "1"); //点击游戏图标
			put("ShowLogin", "2"); //账号登陆界面
			put("Web", "3"); //点击系统公告
			put("ServerList", "4"); //达到选服界面
		}
	};

	@Override
	public boolean execute(List<String> logList) {
		Map<String, List<Map<String, String>>> platformResults = new HashMap<String, List<Map<String, String>>>();
		for (String logStr : logList) {
			JSONObject json = JSONObject.fromObject(logStr);
			if (json != null) {
				String message = json.getString("message");
				if (!message.isEmpty() && !message.equals(" ")) {
					Map<String, String> map = new HashMap<String, String>();
					String hostID = json.getString("hostid");
					map.put("HostID", hostID);
					String time = RegUtils.getLogTime(message);
					map.put("Time", time);
					for (String col : LOG_COLS) {
						String value = RegUtils.getLogValue(message, col, "");
						if(col.equals("Operate")){
							String step = STEP_MAP.getOrDefault(value, "1");
							map.put("Step", step);
							continue;
						}
						map.put(col, value);
					}
					//塞入一些默认值
					map.put("Vfd", "0");
					map.put("Uid", "0");
					map.put("Urs", "");
					String sdkID = RegUtils.getLogValue(message, "SDKId", "");
					String platformID = ServerDB.getPlatformIDBySDKID(sdkID);
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
			AbstractDB db = new ClientLoadLogDB();
			db.batchInsert(platformID, platformResult);
		}
		return true;
	}

}
