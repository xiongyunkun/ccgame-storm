package com.yuhe.mgame.db.statics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.yuhe.mgame.db.DBManager;
/**
 * tblPayDayFrequencyStatics表的相关操作
 * @author xiongyunkun
 *
 */
public class PayDayFrequencyDB {
	private static final String[] DB_COLS = {"PayUserNum", "Pay1Num", "Pay2Num", "Pay3Num",
			"Pay4Num", "Pay5Num", "Pay6Num", "Pay11Num", "Pay20Num" };
	/**
	 * tblPayDayFrequencyStatics表的插入操作
	 * @param platformID
	 * @param hostResults
	 */
	public static void insert(String platformID, String hostID, String date, Map<String, Integer> hostResults) {
		List<String> sqlValues = new ArrayList<String>();
		sqlValues.add("'" + hostID + "'");
		sqlValues.add("'" + date + "'");
		for (String col : DB_COLS) {
			int value = hostResults.getOrDefault(col, 0);
			sqlValues.add("'" + value + "'");
		}
		String sql = "replace into " + platformID + "_statics.tblPayDayFrequencyStatics( HostID, Date, "
				+ StringUtils.join(DB_COLS, ",") + ") values(" + StringUtils.join(sqlValues, ",") + ")";
		DBManager.execute(sql);
	}

}
