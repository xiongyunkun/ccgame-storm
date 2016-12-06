package com.yuhe.mgame.db.statics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.yuhe.mgame.db.DBManager;

/**
 * 插入留存率表相关操作
 * 
 * @author xiongyunkun
 *
 */
public class RetentionDB {
	// 登陆留存统计表相关列
	private static final String[] RETENTION_DB_COLS = { "LoginNum", "NewNum", "1Days", "2Days", "3Days", "4Days",
			"5Days", "6Days", "7Days", "10Days", "13Days", "15Days", "29Days", "30Days" };
	// 付费留存统计表相关列
	private static final String[] PAY_RETENTION_DB_COLS = { "LoginNum", "FirstPayUserNum", "1Days", "2Days", "3Days",
			"4Days", "5Days", "6Days", "7Days", "10Days", "13Days", "15Days", "29Days", "30Days" };

	/**
	 * 插入登陆留存统计表
	 * 
	 * @param platformID
	 * @param hostID
	 * @param date
	 * @param keyValues
	 */
	public static void insertLoginRetention(String platformID, String hostID, String date,
			Map<String, String> keyValues) {
		List<String> cols = new ArrayList<String>();
		List<String> values = new ArrayList<String>();
		List<String> updateValues = new ArrayList<String>();
		values.add(platformID);
		values.add(hostID);
		values.add(date);
		for (String col : RETENTION_DB_COLS) {
			if (keyValues.containsKey(col)) {
				cols.add(col);
				String value = keyValues.get(col);
				values.add(value);
				updateValues.add(col + "= '" + value + "'");
			}
		}
		// 组成sql语句
		String sql = "insert into " + platformID + "_statics.tblRetention(PlatformID, HostID, Date,"
				+ StringUtils.join(cols, ",") + ") values('" + StringUtils.join(values, "','")
				+ "') on duplicate key update " + StringUtils.join(updateValues, ",");
		DBManager.execute(sql);
	}
	/**
	 * 插入付费留存率统计表
	 * @param platformID
	 * @param hostID
	 * @param date
	 * @param keyValues
	 */
	public static void insertPayRetention(String platformID, String hostID, String date,
			Map<String, String> keyValues) {
		List<String> cols = new ArrayList<String>();
		List<String> values = new ArrayList<String>();
		List<String> updateValues = new ArrayList<String>();
		values.add(platformID);
		values.add(hostID);
		values.add(date);
		for (String col : PAY_RETENTION_DB_COLS) {
			if (keyValues.containsKey(col)) {
				cols.add(col);
				String value = keyValues.get(col);
				values.add(value);
				updateValues.add(col + "= '" + value + "'");
			}
		}
		// 组成sql语句
		String sql = "insert into " + platformID + "_statics.tblPayRetention(PlatformID, HostID, Date,"
				+ StringUtils.join(cols, ",") + ") values('" + StringUtils.join(values, "','")
				+ "') on duplicate key update " + StringUtils.join(updateValues, ",");
		DBManager.execute(sql);
	}
	
	/**
	 * 插入登陆留存统计表
	 * 
	 * @param platformID
	 * @param hostID
	 * @param date
	 * @param keyValues
	 */
	public static void insertPhoneRetention(String platformID, String hostID, String date,
			Map<String, String> keyValues) {
		List<String> cols = new ArrayList<String>();
		List<String> values = new ArrayList<String>();
		List<String> updateValues = new ArrayList<String>();
		values.add(platformID);
		values.add(hostID);
		values.add(date);
		for (String col : RETENTION_DB_COLS) {
			if (keyValues.containsKey(col)) {
				cols.add(col);
				String value = keyValues.get(col);
				values.add(value);
				updateValues.add(col + "= '" + value + "'");
			}
		}
		// 组成sql语句
		String sql = "insert into " + platformID + "_statics.tblPhoneRetention(PlatformID, HostID, Date,"
				+ StringUtils.join(cols, ",") + ") values('" + StringUtils.join(values, "','")
				+ "') on duplicate key update " + StringUtils.join(updateValues, ",");
		DBManager.execute(sql);
	}
}
