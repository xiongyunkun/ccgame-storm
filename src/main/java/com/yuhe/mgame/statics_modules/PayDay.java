package com.yuhe.mgame.statics_modules;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;

import com.yuhe.mgame.db.DBManager;
import com.yuhe.mgame.db.ServerDB;
import com.yuhe.mgame.db.log.CommonDB;
import com.yuhe.mgame.db.statics.PayDayStaticsDB;
import com.yuhe.mgame.utils.DateUtils2;

/**
 * 充值区间和服充值统计
 * 
 * @author xiongyunkun
 *
 */
public class PayDay extends AbstractStaticsModule {
	// 记录昨天和今天的充值总额和充值钻石数量
	// 格式：<HostID, <Date, <StaticsType, Number>>>
	private Map<String, Map<String, Map<String, String>>> DayNumMap = new HashMap<String, Map<String, Map<String, String>>>();

	@Override
	public boolean execute(Map<String, List<Map<String, String>>> platformResults) {
		Iterator<String> pIt = platformResults.keySet().iterator();
		while (pIt.hasNext()) {
			String platformID = pIt.next();
			List<Map<String, String>> platformResult = platformResults.get(platformID);
			Map<String, Map<String, Map<String, String>>> serverPayResults = computeServerPayStatics(platformID,
					platformResult);
			Iterator<String> hIt = serverPayResults.keySet().iterator();
			while (hIt.hasNext()) {
				String hostID = hIt.next();
				Map<String, Map<String, String>> hostResult = serverPayResults.get(hostID);
				Iterator<String> dIt = hostResult.keySet().iterator();
				while (dIt.hasNext()) {
					String date = dIt.next();
					Map<String, String> dateResult = hostResult.get(date);
					// 总充值钻石和总充值金额还需要加上昨天的数据
					Map<String, Map<String, String>> hostTotalMap = DayNumMap.get(hostID);
					if (hostTotalMap == null) {
						hostTotalMap = new HashMap<String, Map<String, String>>();
						DayNumMap.put(hostID, hostTotalMap);
					}
					Map<String, String> dateMap = hostTotalMap.get(date);
					if (dateMap == null) {
						dateMap = loadDatePayInfoFromDB(platformID, hostID, date);
						// 充值人数Uid列表也要加上
						Set<String> uids = loadPayUserListPayFromDB(platformID, hostID, date);
						dateMap.put("PayUserUids", StringUtils.join(uids, ",")); // 充值玩家uid列表
						hostTotalMap.put(date, dateMap);
					}
					int totalPayGold = Integer.parseInt(dateResult.get("TotalPayGold"))
							+ Integer.parseInt(dateMap.getOrDefault("TotalPayGold", "0"));
					float totalCashNum = Float.parseFloat(dateResult.get("TotalCashNum"))
							+ Float.parseFloat(dateMap.getOrDefault("TotalCashNum", "0"));
					// 还需要统计总人数
					String[] orgUids = StringUtils.split(dateMap.getOrDefault("PayUserUids", ""), ",");
					Set<String> uidSet = new HashSet<String>(Arrays.asList(orgUids));
					String[] uids = StringUtils.split(dateResult.get("PayUserUids"), ",");
					uidSet.addAll(Arrays.asList(uids));

					dateResult.put("TotalPayGold", Integer.toString(totalPayGold));
					dateResult.put("TotalCashNum", Float.toString(totalCashNum));
					dateResult.put("PayUserNum", Integer.toString(uidSet.size()));
					// 同时也要更新DayNumMap便于下次计算
					dateMap.put("TotalPayGold", Integer.toString(totalPayGold));
					dateMap.put("TotalCashNum", Float.toString(totalCashNum));
					dateMap.put("PayUserUids", StringUtils.join(uidSet, ","));
					// 记录入库
					PayDayStaticsDB.insertPayInfo(platformID, hostID, date, dateResult);

				}
			}
		}
		return true;
	}

	/**
	 * 计算该服该天的充值数据汇总，只针对单服单天
	 * 
	 * @param platformID
	 * @param platformResult
	 */
	private Map<String, Map<String, Map<String, String>>> computeServerPayStatics(String platformID,
			List<Map<String, String>> platformResult) {
		Map<String, Map<String, Map<String, String>>> serverPayResults = new HashMap<String, Map<String, Map<String, String>>>();
		for (Map<String, String> userPay : platformResult) {
			String hostID = userPay.get("HostID");
			String uid = userPay.get("Uid");
			String time = userPay.get("Time");
			String[] times = StringUtils.split(time, " ");
			String date = times[0];
			String cashNum = userPay.get("CashNum");
			String currency = userPay.get("Currency");
			String gold = userPay.get("Gold");
			Map<String, Map<String, String>> serverPayResult = serverPayResults.get(hostID);
			if (serverPayResult == null) {
				serverPayResult = new HashMap<String, Map<String, String>>();
				serverPayResults.put(hostID, serverPayResult);
			}
			Map<String, String> datePayResult = serverPayResult.get(date);
			if (datePayResult == null) {
				datePayResult = new HashMap<String, String>();
				serverPayResult.put(date, datePayResult);
			}
			datePayResult.put("Currency", currency);// 汇率
			float cashNumInt = Float.parseFloat(cashNum) + Float.parseFloat(datePayResult.getOrDefault("CashNum", "0"));
			datePayResult.put("CashNum", Float.toString(cashNumInt));// 充值金额
			int totalGold = Integer.parseInt(gold) + Integer.parseInt(datePayResult.getOrDefault("PayGold", "0"));
			datePayResult.put("PayGold", Integer.toString(totalGold)); // 充值所获钻石
			int payNum = Integer.parseInt(datePayResult.getOrDefault("PayNum", "0"));
			datePayResult.put("PayNum", Integer.toString(payNum + 1)); // 充值次数
			List<String> payUserUids = Arrays
					.asList(StringUtils.split(datePayResult.getOrDefault("PayUserUids", ""), ","));
			if (!payUserUids.contains(uid)) {
				payUserUids.add(uid);
				datePayResult.put("PayUserUids", StringUtils.join(payUserUids, ","));// 充值玩家uid列表
			}
			// 判断该玩家是否是首抽
			Map<String, String> firstPayMap = loadUserPayFormDB(platformID, hostID, uid);
			if (time.equals(firstPayMap.get("FirstCashTime"))) {
				int firstPayUserNum = Integer.parseInt(datePayResult.getOrDefault("FirstPayUserNum", "0"));
				datePayResult.put("FirstPayUserNum", Integer.toString(firstPayUserNum) + 1); // 首充人数
			}
			// 总充值钻石
			int totalPayGold = Integer.parseInt(datePayResult.getOrDefault("TotalPayGold", "0"));
			totalPayGold += Integer.parseInt(gold);
			datePayResult.put("TotalPayGold", Integer.toString(totalPayGold));
			// 总充值金额
			float totalCashNum = Float.parseFloat(datePayResult.getOrDefault("TotalCashNum", "0"));
			totalCashNum += Float.parseFloat(cashNum);
			datePayResult.put("TotalCashNum", Float.toString(totalCashNum));
		}
		return serverPayResults;

	}

	/**
	 * 从tblUserPayStatics表中获得玩家的首抽时间和总充值金额
	 * 
	 * @param platformID
	 * @param hostID
	 * @param uid
	 * @return
	 */
	private Map<String, String> loadUserPayFormDB(String platformID, String hostID, String uid) {
		Map<String, String> result = new HashMap<String, String>();
		String tblName = platformID + "_statics.tblUserPayStatics";
		List<String> options = new ArrayList<String>();
		options.add("Uid = '" + uid + "'");
		Connection conn = DBManager.getConn();
		ResultSet resultSet = CommonDB.query(conn, tblName, options);
		try {
			while (resultSet.next()) {
				result.put("FirstCashTime", resultSet.getString("FirstCashTime"));
				result.put("TotalCashNum", resultSet.getString("TotalCashNum"));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			DBManager.closeConn(conn);
		}
		return result;
	}

	/**
	 * 从tblUserPayDayStatics表中加载获得数据
	 * 
	 * @param platformID
	 * @param hostID
	 * @param date
	 */
	private Set<String> loadPayUserListPayFromDB(String platformID, String hostID, String date) {
		Set<String> uids = new HashSet<String>();
		String tblName = platformID + "_statics.tblUserPayDayStatics";
		List<String> options = new ArrayList<String>();
		options.add("Date = '" + date + "'");
		options.add("HostID = '" + hostID + "'");
		Connection conn = DBManager.getConn();
		ResultSet resultSet = CommonDB.query(conn, tblName, options);
		try {
			while (resultSet.next()) {
				uids.add(resultSet.getString("Uid"));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			DBManager.closeConn(conn);
		}
		return uids;
	}

	/**
	 * 从tblPayDayStatics表中加载昨天的总充值金额和总充值钻石
	 * 
	 * @param platformID
	 * @param hostID
	 * @param date
	 * @return
	 */
	private Map<String, String> loadDatePayInfoFromDB(String platformID, String hostID, String date) {
		Map<String, String> totalNumMap = new HashMap<String, String>();
		String tblName = platformID + "_statics.tblPayDayStatics";
		List<String> options = new ArrayList<String>();
		options.add("Date = '" + date + "'");
		options.add("HostID = '" + hostID + "'");
		Connection conn = DBManager.getConn();
		ResultSet resultSet = CommonDB.query(conn, tblName, options);
		try {
			while (resultSet.next()) {
				totalNumMap.put("TotalPayGold", resultSet.getString("TotalPayGold"));
				totalNumMap.put("TotalCashNum", resultSet.getString("TotalCashNum"));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			DBManager.closeConn(conn);
		}
		return totalNumMap;
	}

	@Override
	public boolean cronExecute() {
		synchronized (DayNumMap) {
			String today = DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd");
			Map<String, String> hostMap = ServerDB.getStaticsServers();
			Iterator<String> hIt = hostMap.keySet().iterator();
			while (hIt.hasNext()) {
				String hostID = hIt.next();
				String platformID = hostMap.get(hostID);
				Map<String, Map<String, String>> hostTotalMap = DayNumMap.get(hostID);
				if (hostTotalMap == null) {
					hostTotalMap = new HashMap<String, Map<String, String>>();
					DayNumMap.put(hostID, hostTotalMap);
				}
				Map<String, String> todayTotalMap = hostTotalMap.get(today);
				if (todayTotalMap == null) {
					/*
					 * 先判断昨天有没有数据，昨天如果有数据就用昨天的充值总额，但是充值uid为空
					 * 如果昨天没有数据，则需要从数据库中获取数据出来
					 */
					todayTotalMap = new HashMap<String, String>();
					String yesterday = DateUtils2.getOverDate(today, -1);
					Map<String, String> yesterdayTotalMap = hostTotalMap.get(yesterday);
					if (yesterdayTotalMap == null) {
						todayTotalMap = loadDatePayInfoFromDB(platformID, hostID, today);
						// 充值人数Uid列表也要加上
						Set<String> uids = loadPayUserListPayFromDB(platformID, hostID, today);
						todayTotalMap.put("PayUserUids", StringUtils.join(uids, ",")); // 充值玩家uid列表
						todayTotalMap.put("PayUserNum", Integer.toString(uids.size())); // 人数也为空
					} else {
						todayTotalMap.put("TotalPayGold", yesterdayTotalMap.getOrDefault("TotalPayGold", "0"));
						todayTotalMap.put("TotalCashNum", yesterdayTotalMap.getOrDefault("TotalCashNum", "0"));
						todayTotalMap.put("PayUserUids", ""); // 充值名单为空
						todayTotalMap.put("PayUserNum", "0"); // 人数也为空
					}
					hostTotalMap.put(today, todayTotalMap);
					// 记录入库
					PayDayStaticsDB.insertPayInfo(platformID, hostID, today, todayTotalMap);
				}

			}
		}
		return true;
	}

}
