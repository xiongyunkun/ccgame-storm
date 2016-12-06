package com.yuhe.mgame.db.statics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;

import com.yuhe.mgame.db.DBManager;

/**
 * 玩家信息表更新
 * 
 * @author xiongyunkun
 * 
 */
public class UserInfoDB {
	/**
	 * 批量插入玩家信息（loginlog日志那里调用）
	 * 
	 * @param platformID
	 * @param results
	 * @return
	 */
	public static boolean batchInsert(String platformID, List<Map<String, String>> results) {
		String[] insertCols = { "HostID", "Uid", "Urs", "Name", "Time", "Sex", "LastUpdateTime" };
		String timeStr = DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss");
		List<String> sqlValues = new ArrayList<String>();
		for (Map<String, String> result : results) {
			List<String> values = new ArrayList<String>();
			for (String col : insertCols) {
				String value = result.get(col);
				value = StringEscapeUtils.escapeSql(value);
				if (col.equals("LastUpdateTime")) {
					value = timeStr;
				}
				values.add(value);
				if(col.equals("Time")){
					values.add(value); //把这个当做注册时间
				}
			}
			sqlValues.add(StringUtils.join(values, "','"));
		}
		String[] cols = { "HostID", "Uid", "Urs", "Name", "LastLoginTime", "RegTime", "Sex", "LastUpdateTime" };
		StringBuilder sb = new StringBuilder();
		sb.append("insert into ").append(platformID).append("_statics.tblUserInfo(").append(StringUtils.join(cols, ","))
				.append(") values('").append(StringUtils.join(sqlValues, "'),('"))
				.append("') on duplicate key update Name = values(Name),LastLoginTime=values(LastLoginTime),LastUpdateTime=values(LastUpdateTime),OnlineFlag='1'");
		DBManager.execute(sb.toString());
		return true;
	}

	/**
	 * 批量更新玩家信息（供logout日志调用)
	 * 
	 * @param platformID
	 * @param results
	 * @return
	 */
	public static boolean batchUpdate(String platformID, List<Map<String, String>> results) {
		String[] updateCols = { "HostID", "Uid", "Urs", "Name", "Level","LastLogoutTime","SceneUid",
				"TotalOnlineTime","Fighting","Gold","TotalGold","Money","OnlineFlag","LastUpdateTime","VipLevel", "IsVip" };
		String[] duplicateCols = { "Name", "Urs", "Level","LastLogoutTime","SceneUid","Fighting",
				"Gold","TotalGold","Money","OnlineFlag","LastUpdateTime","VipLevel", "IsVip" };
		Map<String, String> colMap = new HashMap<String, String>();
		colMap.put("LastLogoutTime", "Time");
		colMap.put("Level", "Lv");
		colMap.put("TotalOnlineTime", "OnTime");
		String timeStr = DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss");
		Map<String, String> defaultValues = new HashMap<String, String>();
		defaultValues.put("OnlineFlag", "0");
		defaultValues.put("LastUpdateTime", timeStr);

		List<String> sqlValues = new ArrayList<String>();
		for (Map<String, String> result : results) {
			List<String> values = new ArrayList<String>();
			for (String col : updateCols) {
				String value = result.get(col);
				if (colMap.containsKey(col)) {
					value = result.get(colMap.get(col));
				} else if (defaultValues.containsKey(col)) {
					value = defaultValues.get(col);
				}
				value = StringEscapeUtils.escapeSql(value);
				values.add(value);
			}
			sqlValues.add(StringUtils.join(values, "','"));
		}
		List<String> duplicates = new ArrayList<String>();
		for (String col : duplicateCols) {
			duplicates.add(col + "=values(" + col + ")");
		}
		StringBuilder sb = new StringBuilder();
		sb.append("insert into ").append(platformID).append("_statics.tblUserInfo(")
				.append(StringUtils.join(updateCols, ",")).append(") values('")
				.append(StringUtils.join(sqlValues, "'),('")).append("') on duplicate key update ")
				.append(StringUtils.join(duplicates, ","))
				.append(",TotalOnlineTime = TotalOnlineTime + values(TotalOnlineTime)");
		DBManager.execute(sb.toString());
		return true;
	}

	/**
	 * 批量更新名字
	 * 
	 * @param platformID
	 * @param results
	 * @return
	 */
	public static boolean batchUpdateName(String platformID, List<Map<String, String>> results) {
		String[] nameCols = { "HostID", "Uid", "Urs", "Name" };
		List<String> sqlValues = new ArrayList<String>();
		for (Map<String, String> result : results) {
			List<String> values = new ArrayList<String>();
			for (String nameCol : nameCols) {
				String value = result.get(nameCol);
				value = StringEscapeUtils.escapeSql(value);
				values.add(value);
			}
			sqlValues.add(StringUtils.join(values, "','"));
		}
		StringBuilder sb = new StringBuilder();
		sb.append("insert into ").append(platformID).append("_statics.tblUserInfo(")
				.append(StringUtils.join(nameCols, ",")).append(") values('").append(StringUtils.join(sqlValues, "'),('"))
				.append("') on duplicate key update Name = values(Name)");
		DBManager.execute(sb.toString());
		return true;
	}

	/**
	 * 批量更新注册时间
	 * 
	 * @param platformID
	 * @param results
	 * @return
	 */
	public static boolean batchUpdateRegTime(String platformID, List<Map<String, String>> results) {
		String[] nameCols = { "HostID", "Uid", "Urs", "Time" };
		List<String> sqlValues = new ArrayList<String>();
		for (Map<String, String> result : results) {
			List<String> values = new ArrayList<String>();
			for (String nameCol : nameCols) {
				String value = result.get(nameCol);
				value = StringEscapeUtils.escapeSql(value);
				values.add(value);
			}
			sqlValues.add(StringUtils.join(values, "','"));
		}
		StringBuilder sb = new StringBuilder();
		sb.append("insert into ").append(platformID).append("_statics.tblUserInfo(")
				.append(StringUtils.join(nameCols, ",")).append(") values(").append(StringUtils.join(sqlValues, "),("))
				.append(") on duplicate key update RegTime = values(RegTime)");
		DBManager.execute(sb.toString());
		return true;
	}

	/**
	 * 批量更新等级
	 * 
	 * @param platformID
	 * @param results
	 * @return
	 */
	public static boolean batchUpdateLevel(String platformID, List<Map<String, String>> results) {
		String[] nameCols = { "HostID", "Uid", "Urs", "Level", "Name" };
		List<String> sqlValues = new ArrayList<String>();
		for (Map<String, String> result : results) {
			List<String> values = new ArrayList<String>();
			for (String nameCol : nameCols) {
				String value = result.getOrDefault(nameCol, "");
				if(nameCol.equals("Level")){
					value = result.getOrDefault("NewLevel", "");
				}
				value = StringEscapeUtils.escapeSql(value);
				values.add(value);
			}
			sqlValues.add(StringUtils.join(values, "','"));
		}
		StringBuilder sb = new StringBuilder();
		sb.append("insert into ").append(platformID).append("_statics.tblUserInfo(")
				.append(StringUtils.join(nameCols, ",")).append(") values('").append(StringUtils.join(sqlValues, "'),('"))
				.append("') on duplicate key update Level = values(Level), Name= values(Name)");
		DBManager.execute(sb.toString());
		return true;
	}
}
