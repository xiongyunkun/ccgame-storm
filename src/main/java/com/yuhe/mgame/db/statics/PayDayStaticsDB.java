package com.yuhe.mgame.db.statics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.yuhe.mgame.db.DBManager;

public class PayDayStaticsDB {
	private static final String[] PAY_COLS = { "Currency", "CashNum", "PayGold", "PayNum", "PayUserNum",
			"FirstPayUserNum", "TotalCashNum", "TotalPayGold" };
	private static final String[] PAY_UP_COLS = { "CashNum", "PayGold", "PayNum", "FirstPayUserNum" };
	private static final String[] GOLD_COLS = { "GoldConsume", "GoldProduce", "TotalGoldProduce", "TotalGoldConsume",
			"TotalCreditGoldProduce", "TotalCreditGoldConsume", "CreditGoldProduce", "CreditGoldConsume" };
	private static final String[] GOLD_UP_CPLS = { "GoldConsume", "GoldProduce", "CreditGoldProduce",
			"CreditGoldConsume" };

	/**
	 * 将充值统计数据插入数据库
	 * 
	 * @param platformID
	 * @param hostID
	 * @param date
	 * @param results
	 * @return
	 */
	public static boolean insertPayInfo(String platformID, String hostID, String date, Map<String, String> results) {
		List<String> sqlValues = new ArrayList<String>();
		sqlValues.add("'" + hostID + "'");
		sqlValues.add("'" + date + "'");
		for (String col : PAY_COLS) {
			String value = results.getOrDefault(col, "0");
			sqlValues.add("'" + value + "'");
		}
		List<String> updateValues = new ArrayList<String>();
		updateValues.add("Currency = values(Currency)");
		updateValues.add("PayUserNum = values(PayUserNum)");
		updateValues.add("TotalCashNum = values(TotalCashNum)");
		updateValues.add("TotalPayGold = values(TotalPayGold)");
		for (String col : PAY_UP_COLS) {
			updateValues.add(col + "= " + col + "+ values(" + col + ")");
		}
		String sql = "insert into " + platformID + "_statics.tblPayDayStatics(HostID,Date,"
				+ StringUtils.join(PAY_COLS, ",") + ") values(" + StringUtils.join(sqlValues, ",")
				+ ") on duplicate key update " + StringUtils.join(updateValues, ",");
		DBManager.execute(sql);
		return true;
	}

	/**
	 * 将钻石统计插入数据库
	 * 
	 * @param platformID
	 * @param hostID
	 * @param date
	 * @param results
	 * @return
	 */
	public static boolean insertGoldInfo(String platformID, String hostID, String date, Map<String, Integer> results) {
		List<String> sqlValues = new ArrayList<String>();
		sqlValues.add("'" + hostID + "'");
		sqlValues.add("'" + date + "'");
		for (String col : GOLD_COLS) {
			int value = results.getOrDefault(col, 0);
			sqlValues.add("'" + value + "'");
		}
		List<String> updateValues = new ArrayList<String>();
		updateValues.add("TotalGoldProduce = values(TotalGoldProduce)");
		updateValues.add("TotalGoldConsume = values(TotalGoldConsume)");
		updateValues.add("TotalCreditGoldProduce = values(TotalCreditGoldProduce)");
		updateValues.add("TotalCreditGoldConsume = values(TotalCreditGoldConsume)");
		for (String col : GOLD_UP_CPLS) {
			updateValues.add(col + "= " + col + " + values(" + col + ")");
		}
		String sql = "insert into " + platformID + "_statics.tblPayDayStatics(HostID,Date,"
				+ StringUtils.join(GOLD_COLS, ",") + ") values(" + StringUtils.join(sqlValues, ",")
				+ ") on duplicate key update " + StringUtils.join(updateValues, ",");
		DBManager.execute(sql);
		return true;
	}
}
