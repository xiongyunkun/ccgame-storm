package com.yuhe.mgame.db.statics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.shade.org.apache.commons.lang.StringUtils;

import com.yuhe.mgame.db.DBManager;

/**
 * 在线时长统计表相关操作
 * 
 * @author xiongyunkun
 *
 */
public class OnlineTimeDB {
	// 对应列
	private static final String[] DB_COLS = { "Time0", "Time1", "Time5", "Time10", "Time15", "Time30", "Time45",
			"Time60", "Time90", "Time120", "Time150", "Time180", "Time240", "Time300", "Time360", "Time420", "Time600",
			"Time900", "Time1200", "TotalTimes" };

	/**
	 * 插入在线时长统计表
	 * 
	 * @param platformID
	 * @param hostID
	 * @param date
	 * @param userType
	 * @param totalPlayers
	 * @param timeMap
	 */
	public static void insert(String platformID, String hostID, String date, int userType, int totalPlayers,
			Map<String, Integer> timeMap) {
		StringBuilder sql = new StringBuilder();
		sql.append("insert into ").append(platformID)
				.append("_statics.tblOnlineTime(PlatformID, HostID, Date, UserType, TotalPlayers,")
				.append(StringUtils.join(DB_COLS, ",")).append(") values('").append(platformID).append("','")
				.append(hostID).append("','").append(date).append("','").append(userType).append("','")
				.append(totalPlayers).append("','");
		List<String> valueList = new ArrayList<String>();
		List<String> updateValueList = new ArrayList<String>();
		updateValueList.add("TotalPlayers='" + totalPlayers + "'");
		for (String col : DB_COLS) {
			int value = timeMap.getOrDefault(col, 0);
			
			valueList.add(Integer.toString(value));
			
			if (timeMap.containsKey(col)) {
				updateValueList.add(col + "='" + value + "'");
			}
		}
		sql.append(StringUtils.join(valueList, "','")).append("') on duplicate key update ")
				.append(StringUtils.join(updateValueList, ","));
		DBManager.execute(sql.toString());
	}
}
