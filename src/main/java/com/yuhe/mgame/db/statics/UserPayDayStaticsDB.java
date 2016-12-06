package com.yuhe.mgame.db.statics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.yuhe.mgame.db.DBManager;
/**
 * tblUserPayDayStatics表相关操作
 * @author xiongyunkun
 *
 */
public class UserPayDayStaticsDB {
	private static final String[] DB_COLS = { "Uid", "Date", "Urs", "Name", "HostID", "Currency", "TotalCashNum",
			"TotalNum", "TotalGold" };
	/**
	 * 插入tblUserPayDayStatics表
	 * @param platformID
	 * @param platformResult
	 */
	public static void insert(String platformID, List<Map<String, String>> platformResult) {
		List<String> sqlValues = new ArrayList<String>();
		for (Map<String, String> values : platformResult) {
			List<String> colValues = new ArrayList<String>();
			for (String col : DB_COLS) {
				String value = values.getOrDefault(col, "");
				colValues.add("'" + value + "'");
			}
			sqlValues.add(StringUtils.join(colValues, ","));
		}
		String sql = "insert into " + platformID + "_statics.tblUserPayDayStatics( " + StringUtils.join(DB_COLS, ",")
				+ ") values(" + StringUtils.join(sqlValues, "),(")
				+ ") on duplicate key update TotalCashNum = TotalCashNum + values(TotalCashNum), TotalNum = TotalNum + values(TotalNum),TotalGold = TotalGold + values(TotalGold)";
		DBManager.execute(sql);
	}
}
