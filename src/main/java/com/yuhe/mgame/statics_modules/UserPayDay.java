package com.yuhe.mgame.statics_modules;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.yuhe.mgame.db.DBManager;
import com.yuhe.mgame.db.log.CommonDB;
import com.yuhe.mgame.db.statics.PayDayFrequencyDB;
import com.yuhe.mgame.db.statics.UserPayDayStaticsDB;
import com.yuhe.mgame.utils.DateUtils2;
/**
 * 玩家单日充值统计和充值频率统计
 * @author xiongyunkun
 *
 */
public class UserPayDay extends AbstractStaticsModule {
	// 记录当天服的每个人的充值次数
	// 数据格式：<HostID, <date, <Type, Number>>>
	private Map<String, Map<String, Map<String, Integer>>> PayNumMap = new HashMap<String, Map<String, Map<String, Integer>>>();
	// 记录当天服的充值频率
	// 数据格式：<HostID, <Date, <FrequencyType, Number>>>
	private Map<String, Map<String, Map<String, Integer>>> FrequencyMap = new HashMap<String, Map<String, Map<String, Integer>>>();
	// 充值频率区间
	private static final Map<String, int[]> ZONE_MAP = new HashMap<String, int[]>() {
		private static final long serialVersionUID = 1L;

		{
			put("Pay1Num", new int[] { 1, 1 });
			put("Pay2Num", new int[] { 2, 2 });
			put("Pay3Num", new int[] { 3, 3 });
			put("Pay4Num", new int[] { 4, 4 });
			put("Pay5Num", new int[] { 5, 5 });
			put("Pay6Num", new int[] { 6, 10 });
			put("Pay11Num", new int[] { 11, 20 });
			put("Pay20Num", new int[] { 21 });
		}

	};

	@Override
	public boolean execute(Map<String, List<Map<String, String>>> platformResults) {
		Iterator<String> pIt = platformResults.keySet().iterator();
		while (pIt.hasNext()) {
			String platformID = pIt.next();
			List<Map<String, String>> platformResult = platformResults.get(platformID);
			Map<String, Map<String, Map<String, String>>> userPayResults = new HashMap<String, Map<String, Map<String, String>>>();
			ComputeUserPayDay(platformResult, userPayResults);
			// 记录数据库
			List<Map<String, String>> userPayList = new ArrayList<Map<String, String>>();
			Iterator<String> it = userPayResults.keySet().iterator();
			while (it.hasNext()) {
				String uid = it.next();
				Map<String, Map<String, String>> datePayResult = userPayResults.get(uid);
				Iterator<String> dIt = datePayResult.keySet().iterator();
				while (dIt.hasNext()) {
					String date = dIt.next();
					userPayList.add(datePayResult.get(date));
				}
			}
			UserPayDayStaticsDB.insert(platformID, userPayList);
			// 再统计充值频率
			Set<String> strSet = ComputeFrequency(platformID, platformResult);
			for (String str : strSet) {
				String[] strs = StringUtils.split(str, "_");
				if (strs.length == 2) {
					String hostID = strs[0];
					String date = strs[1];
					Map<String, Integer> hostFrequecyMap = FrequencyMap.get(hostID).get(date);
					PayDayFrequencyDB.insert(platformID, hostID, date, hostFrequecyMap);
				}
			}
		}
		return true;
	}

	/**
	 * 统计玩家个人单日充值情况
	 * 
	 * @param platformResult
	 * @param userPayResults
	 */
	private void ComputeUserPayDay(List<Map<String, String>> platformResult,
			Map<String, Map<String, Map<String, String>>> userPayResults) {
		for (Map<String, String> userPay : platformResult) {
			String uid = userPay.get("Uid");
			Map<String, Map<String, String>> datePayResult = userPayResults.get(uid);
			if (datePayResult == null) {
				datePayResult = new HashMap<String, Map<String, String>>();
				userPayResults.put(uid, datePayResult);
			}
			String time = userPay.get("Time");
			String[] times = StringUtils.split(time, " ");
			String date = times[0];
			Map<String, String> userPayResult = datePayResult.get(date);
			if (userPayResult == null) {
				userPayResult = new HashMap<String, String>();
				datePayResult.put(date, userPayResult);
			}
			// 计算总充值金额
			String cashNum = userPay.get("CashNum");
			float totalCashNum = Float.parseFloat(cashNum)
					+ Float.parseFloat(userPayResult.getOrDefault("TotalCashNum", "0"));
			// 计算总充值次数
			int totalNum = Integer.parseInt(userPayResult.getOrDefault("TotalNum", "0")) + 1;
			// 计算总充值钻石
			String gold = userPay.get("Gold");
			int totalGold = Integer.parseInt(gold) + Integer.parseInt(userPayResult.getOrDefault("TotalGold", "0"));
			userPayResult.put("Uid", uid);
			userPayResult.put("Urs", userPay.get("Urs"));
			userPayResult.put("Name", userPay.get("Name"));
			userPayResult.put("HostID", userPay.get("HostID"));
			userPayResult.put("Date", date);
			userPayResult.put("Currency", userPay.get("Currency"));
			userPayResult.put("TotalCashNum", Float.toString(totalCashNum));
			userPayResult.put("TotalNum", Integer.toString(totalNum));
			userPayResult.put("TotalGold", Integer.toString(totalGold));
		}
	}

	/**
	 * 统计充值频率
	 * 
	 * @param platformID
	 * @param platformResult
	 */
	private Set<String> ComputeFrequency(String platformID, List<Map<String, String>> platformResult) {
	Set<String> strSet = new HashSet<String>();
		for (Map<String, String> userPay : platformResult) {
			String hostID = userPay.get("HostID");
			String uid = userPay.get("Uid");
			String time = userPay.get("Time");
			String[] times = StringUtils.split(time, " ");
			String date = times[0];
			if (!PayNumMap.containsKey(hostID)) {
				loadUserPayDayStaticsFromDB(platformID, hostID, date);
			}
			Map<String, Map<String, Integer>> hostNumResult = PayNumMap.get(hostID);
			Map<String, Integer> dateNumResult = hostNumResult.get(date);
			if (dateNumResult == null) {
				dateNumResult = new HashMap<String, Integer>();
				// 把除了date这一天和前一天的日期之外的日期的数据都清空
				String yesterday = DateUtils2.getOverDate(date, -1);
				Iterator<String> dIt = hostNumResult.keySet().iterator();
				while (dIt.hasNext()) {
					String tDate = dIt.next();
					if (!tDate.equals(yesterday)) {
						dIt.remove();
					}
				}
				hostNumResult.put(date, dateNumResult);
				PayNumMap.put(hostID, hostNumResult);
				// 充值频率那里也要清空
				Map<String, Integer> dateFreResult = new HashMap<String, Integer>();
				Iterator<String> fIt = FrequencyMap.get(hostID).keySet().iterator();
				while (fIt.hasNext()) {
					String tDate = fIt.next();
					if (!tDate.equals(yesterday)) {
						fIt.remove();
					}
				}
				FrequencyMap.get(hostID).put(date, dateFreResult);
			}
			int oldValue = dateNumResult.getOrDefault(uid, 0);
			int newValue = oldValue + 1;
			dateNumResult.put(uid, oldValue + 1);
			// 更新频率统计区间
			String oldFrequencyID = getFrequencyID(oldValue);
			String newFrequencyID = getFrequencyID(newValue);
			Map<String, Map<String, Integer>> hostFrequencyResult = FrequencyMap.get(hostID);
			Map<String, Integer> dateFreResult = hostFrequencyResult.get(date);
			if (oldFrequencyID == null) {
				int newFrequencyNum = dateFreResult.getOrDefault(newFrequencyID, 0);
				dateFreResult.put(newFrequencyID, newFrequencyNum + 1);
			} else if (!oldFrequencyID.equals(newFrequencyID)) {
				int oldFrquencyNum = dateFreResult.get(oldFrequencyID);
				int newFrequencyNum = dateFreResult.getOrDefault(newFrequencyID, 0);
				dateFreResult.put(oldFrequencyID, Math.max(0, oldFrquencyNum - 1));
				dateFreResult.put(newFrequencyID, newFrequencyNum + 1);
			}
			dateFreResult.put("PayUserNum", dateNumResult.keySet().size()); // 重新计算充值总人数
			strSet.add(hostID + "_" + date);
		}
		return strSet;
	}

	/**
	 * 从数据库中加载数据
	 * 
	 * @param platformID
	 * @param hostID
	 * @param date
	 */
	private void loadUserPayDayStaticsFromDB(String platformID, String hostID, String date) {
		Map<String, Integer> userPayStatics = new HashMap<String, Integer>();
		Map<String, Integer> frequencyStatics = new HashMap<String, Integer>();
		String tblName = platformID + "_statics.tblUserPayDayStatics";
		List<String> options = new ArrayList<String>();
		options.add("HostID = '" + hostID + "'");
		options.add("Date = '" + date + "'");
		Connection conn = DBManager.getConn();
		ResultSet resultSet = CommonDB.query(conn, tblName, options);
		try {
			while (resultSet.next()) {
				String uid = resultSet.getString("Uid");
				int totalNum = resultSet.getInt("TotalNum");
				userPayStatics.put(uid, totalNum);
				String frequencyID = getFrequencyID(totalNum);
				frequencyStatics.put(frequencyID, frequencyStatics.getOrDefault(frequencyID, 0)+1);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			DBManager.closeConn(conn);
		}
		// 将统计数据放入PayNumMap和FrequencyMap中
		Map<String, Map<String, Integer>> dateResult = new HashMap<String, Map<String, Integer>>();
		dateResult.put(date, userPayStatics);
		PayNumMap.put(hostID, dateResult);
		// 同时也要更新到统计频率里面
		Map<String, Map<String, Integer>> dateFrequencyResult = new HashMap<String, Map<String, Integer>>();
		dateFrequencyResult.put(date, frequencyStatics);
		FrequencyMap.put(hostID, dateFrequencyResult);
	}

	/**
	 * 根据充值次数获得充值区间ID
	 * 
	 * @param number
	 * @return
	 */
	private String getFrequencyID(int number) {
		String frequencyID = null;
		Iterator<String> it = ZONE_MAP.keySet().iterator();
		while (it.hasNext()) {
			frequencyID = it.next();
			int[] zones = ZONE_MAP.get(frequencyID);
			if (zones.length == 2 && number >= zones[0] && number <= zones[1]) {
				break;
			} else if (zones.length == 1 && number >= zones[0]) {
				break;
			}
		}
		return frequencyID;
	}

	@Override
	public boolean cronExecute() {
		// TODO Auto-generated method stub
		return false;
	}

}
