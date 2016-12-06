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

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;

import com.yuhe.mgame.db.DBManager;
import com.yuhe.mgame.db.ServerDB;
import com.yuhe.mgame.db.log.CommonDB;
import com.yuhe.mgame.db.statics.RetentionDB;
import com.yuhe.mgame.utils.DateUtils2;
import com.yuhe.mgame.utils.RegUtils;

/**
 * 统计留存率，付费留存率数据,设备留存
 * 
 * @author xiongyunkun
 *
 */
public class Retention extends AbstractStaticsModule {
	// 记录历史注册数据，数据格式：Map<HostID, Map<Date,Set<Uid>>>
	private static Map<String, Map<String, Set<String>>> RegResults = new HashMap<String, Map<String, Set<String>>>();
	// 记录首充人数，数据格式:Map<HostID, Map<Date,Set<Uid>>>
	private static Map<String, Map<String, Set<String>>> FirstPayResults = new HashMap<String, Map<String, Set<String>>>();
	// 记录登陆的玩家Uid，数据格式：Map<HostID, Map<Date,Set<Uid>>>,这里记录今天和昨天的数据,前天之前的数据都会删除
	private static Map<String, Map<String, Set<String>>> LoginDayUids = new HashMap<String, Map<String, Set<String>>>();
	// 记录每次统计周期内的登陆玩家uid，统计完后会清空，数据格式：Map<PlatformID, Map<HostID,
	// Map<Date,Set<Uid>>>>
	private Map<String, Map<String, Map<String, Set<String>>>> PeriodLoginUids = new HashMap<String, Map<String, Map<String, Set<String>>>>();
	// 记录历史注册IMEI设备号，数据格式：Map<HostID, Map<Date,Set<Uid>>>
	private static Map<String, Map<String, Set<String>>> RegIMEIResults = new HashMap<String, Map<String, Set<String>>>();
	// 记录登陆玩家的设备号，数据格式：Map<HostID, Map<Date,Set<Uid>>>,这里记录今天和昨天的数据,前天之前的数据都会删除
	private static Map<String, Map<String, Set<String>>> LoginDayIMEIs = new HashMap<String, Map<String, Set<String>>>();
	// 记录每次统计周期内的登陆玩家IMEI，统计完后会清空，数据格式：Map<PlatformID, Map<HostID,
	// Map<Date,Set<IMEI>>>>
	private Map<String, Map<String, Map<String, Set<String>>>> PeriodLoginIMEIs = new HashMap<String, Map<String, Map<String, Set<String>>>>();

	@Override
	public boolean execute(Map<String, List<Map<String, String>>> platformResults) {
		Map<String, Map<String, Map<String, Map<String, Set<String>>>>> platformInfo = getLoginInfo(platformResults);
		Map<String, Map<String, Map<String, Set<String>>>> platformUids = platformInfo.get("Uid");
		mergeTodayData(LoginDayUids, platformUids, "Uid");
		Map<String, Map<String, Map<String, Set<String>>>> platformIMEIs = platformInfo.get("IMEI");
		mergeTodayData(LoginDayIMEIs, platformIMEIs, "IMEI");
		mergePeriodData(PeriodLoginUids, platformUids);
		mergePeriodData(PeriodLoginIMEIs, platformIMEIs);
		return true;
	}

	/**
	 * 从登陆和登出日志中获得Uid和IMEI设备号,并且uid和IMEI进行过滤去重
	 * 
	 * @param platformResults
	 * @return
	 */
	private Map<String, Map<String, Map<String, Map<String, Set<String>>>>> getLoginInfo(
			Map<String, List<Map<String, String>>> platformResults) {
		Map<String, Map<String, Map<String, Set<String>>>> platformUids = new HashMap<String, Map<String, Map<String, Set<String>>>>();
		Map<String, Map<String, Map<String, Set<String>>>> platformIMEIs = new HashMap<String, Map<String, Map<String, Set<String>>>>();
		Iterator<String> pIt = platformResults.keySet().iterator();
		while (pIt.hasNext()) {
			String platformID = pIt.next();
			List<Map<String, String>> platformResult = platformResults.get(platformID);
			Map<String, Map<String, Set<String>>> hostResults = platformUids.get(platformID);
			Map<String, Map<String, Set<String>>> hostIMEIResults = platformIMEIs.get(platformID);
			if (hostResults == null) {
				hostResults = new HashMap<String, Map<String, Set<String>>>();
				platformUids.put(platformID, hostResults);
				hostIMEIResults = new HashMap<String, Map<String, Set<String>>>();
				platformIMEIs.put(platformID, hostIMEIResults);
			}
			for (Map<String, String> hostResult : platformResult) {
				String hostID = hostResult.get("HostID");
				String uid = hostResult.get("Uid");
				String time = hostResult.get("Time");
				String[] times = StringUtils.split(time, " ");
				String date = times[0];
				String phoneInfo = hostResult.get("PhoneInfo");
				String imei = RegUtils.getIMEI(phoneInfo);
				Map<String, Set<String>> dateResults = hostResults.get(hostID);
				Map<String, Set<String>> dateIMEIResults = hostIMEIResults.get(hostID);
				if (dateResults == null) {
					dateResults = new HashMap<String, Set<String>>();
					hostResults.put(hostID, dateResults);
					dateIMEIResults = new HashMap<String, Set<String>>();
					hostIMEIResults.put(hostID, dateIMEIResults);
				}
				Set<String> uidSet = dateResults.get(date);
				Set<String> imeiSet = dateIMEIResults.get(date);
				if (uidSet == null) {
					uidSet = new HashSet<String>();
					dateResults.put(date, uidSet);
					imeiSet = new HashSet<String>();
					dateIMEIResults.put(date, imeiSet);
				}
				uidSet.add(uid);
				imeiSet.add(imei);
			}
		}
		Map<String, Map<String, Map<String, Map<String, Set<String>>>>> loginInfoMap = new HashMap<String, Map<String, Map<String, Map<String, Set<String>>>>>();
		loginInfoMap.put("Uid", platformUids);
		loginInfoMap.put("IMEI", platformIMEIs);
		return loginInfoMap;
	}

	/**
	 * 合并当前的数据到todayData中
	 * 
	 * @param todayData
	 * @param nowData
	 */
	private void mergeTodayData(Map<String, Map<String, Set<String>>> todayData,
			Map<String, Map<String, Map<String, Set<String>>>> nowData, String type) {
		Iterator<String> pfIt = nowData.keySet().iterator();
		while (pfIt.hasNext()) {
			String platformID = pfIt.next();
			Map<String, Map<String, Set<String>>> loginUids = nowData.get(platformID);
			Iterator<String> pIt = loginUids.keySet().iterator();
			while (pIt.hasNext()) {
				String hostID = pIt.next();
				Map<String, Set<String>> hostResults = todayData.get(hostID);
				if (hostResults == null) {
					hostResults = new HashMap<String, Set<String>>();
					todayData.put(hostID, hostResults);
				}
				Map<String, Set<String>> hostUids = loginUids.get(hostID);
				Iterator<String> hIt = hostUids.keySet().iterator();
				while (hIt.hasNext()) {
					String date = hIt.next();
					Set<String> uids = hostUids.get(date);
					Set<String> dayUids = hostResults.get(date);
					if (dayUids == null) {
						Map<String, Set<String>> loginDataMap = loadLoginUidFromDB(platformID, hostID, date);
						Map<String, Set<String>> logoutDataMap = loadLogoutUidFromDB(platformID, hostID, date);
						dayUids = loginDataMap.get(type);
						dayUids.addAll(logoutDataMap.get(type));
						hostResults.put(date, dayUids);
						// 今天没有数据，记录今天数据的同时需要将除了今天和昨天的数据都要删除
						String yesterday = DateUtils2.getOverDate(date, -1);
						Iterator<String> dIt = hostResults.keySet().iterator();
						while (dIt.hasNext()) {
							String tDate = dIt.next();
							if (!tDate.equals(date) && !tDate.equals(yesterday)) {
								dIt.remove(); // 除去今天和昨天的都要删除
							}
						}
					}
					dayUids.addAll(uids);
				}
			}
		}
	}

	/**
	 * 合并当前的数据到当前统计周期的数据periodData中
	 * 
	 * @param periodData
	 * @param nowData
	 */
	private void mergePeriodData(Map<String, Map<String, Map<String, Set<String>>>> periodData,
			Map<String, Map<String, Map<String, Set<String>>>> nowData) {
		Iterator<String> pIt = nowData.keySet().iterator();
		while (pIt.hasNext()) {
			String platformID = pIt.next();
			Map<String, Map<String, Set<String>>> pLoginUids = periodData.get(platformID);
			if (pLoginUids == null) {
				pLoginUids = new HashMap<String, Map<String, Set<String>>>();
				periodData.put(platformID, pLoginUids);
			}
			Iterator<String> hIt = nowData.get(platformID).keySet().iterator();
			while (hIt.hasNext()) {
				String hostID = hIt.next();
				Map<String, Set<String>> hLoginUids = pLoginUids.get(hostID);
				if (hLoginUids == null) {
					hLoginUids = new HashMap<String, Set<String>>();
					pLoginUids.put(hostID, hLoginUids);
				}
				Iterator<String> dIt = nowData.get(platformID).get(hostID).keySet().iterator();
				while (dIt.hasNext()) {
					String date = dIt.next();
					Set<String> dLoginUids = hLoginUids.get(date);
					if (dLoginUids == null) {
						dLoginUids = new HashSet<String>();
						hLoginUids.put(date, dLoginUids);
					}
					dLoginUids.addAll(nowData.get(platformID).get(hostID).get(date));
				}
			}
		}
	}

	/**
	 * 从数据库中加载当天的登陆玩家的uid和IMEI
	 * 
	 * @param platformID
	 * @param hostID
	 * @param date
	 * @return
	 */
	private static Map<String, Set<String>> loadLoginUidFromDB(String platformID, String hostID, String date) {
		Set<String> uids = new HashSet<String>();
		Set<String> imeis = new HashSet<String>();
		String tblName = platformID + "_log.tblLoginLog_" + date.replace("-", "");
		List<String> options = new ArrayList<String>();
		options.add("HostID = '" + hostID + "'");
		options.add("Time >= '" + date + " 00:00:00'");
		options.add("Time <= '" + date + " 23:59:59'");
		Connection conn = DBManager.getConn();
		ResultSet resultSet = CommonDB.query(conn, tblName, options);
		try {
			while (resultSet.next()) {
				String uid = resultSet.getString("Uid");
				uids.add(uid);
				String phoneInfo = resultSet.getString("PhoneInfo");
				String imei = RegUtils.getIMEI(phoneInfo);
				imeis.add(imei);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			DBManager.closeConn(conn);
		}
		Map<String, Set<String>> results = new HashMap<String, Set<String>>();
		results.put("Uid", uids);
		results.put("IMEI", imeis);
		return results;
	}

	/**
	 * 从数据库中加载当天的登出玩家的uid和IMEI
	 * 
	 * @param platformID
	 * @param hostID
	 * @param date
	 * @return
	 */
	private static Map<String, Set<String>> loadLogoutUidFromDB(String platformID, String hostID, String date) {
		Set<String> uids = new HashSet<String>();
		Set<String> imeis = new HashSet<String>();
		String tblName = platformID + "_log.tblLogoutLog_" + date.replace("-", "");
		List<String> options = new ArrayList<String>();
		options.add("HostID = '" + hostID + "'");
		options.add("Time >= '" + date + " 00:00:00'");
		options.add("Time <= '" + date + " 23:59:59'");
		Connection conn = DBManager.getConn();
		ResultSet resultSet = CommonDB.query(conn, tblName, options);
		try {
			while (resultSet.next()) {
				String uid = resultSet.getString("Uid");
				uids.add(uid);
				String phoneInfo = resultSet.getString("PhoneInfo");
				String imei = RegUtils.getIMEI(phoneInfo);
				imeis.add(imei);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			DBManager.closeConn(conn);
		}
		Map<String, Set<String>> results = new HashMap<String, Set<String>>();
		results.put("Uid", uids);
		results.put("IMEI", imeis);
		return results;
	}

	/**
	 * 返回该服该天的注册Uid列表和设备号IMEI列表、
	 * 如果该列表为none，则从数据库中加载数据，并缓存在RegResults和RegIMEIResults中
	 * 
	 * @param platformID
	 * @param hostID
	 * @param date
	 */
	private static Map<String, Set<String>> loadRegMap(String platformID, String hostID, String date) {
		Map<String, Set<String>> regMap = new HashMap<String, Set<String>>();
		Map<String, Set<String>> hostRegUids = RegResults.get(hostID);
		if (hostRegUids == null) {
			hostRegUids = new HashMap<String, Set<String>>();
			RegResults.put(hostID, hostRegUids);
		}
		Map<String, Set<String>> hostRegIMEIs = RegIMEIResults.get(hostID);
		if (hostRegIMEIs == null) {
			hostRegIMEIs = new HashMap<String, Set<String>>();
			RegIMEIResults.put(hostID, hostRegIMEIs);
		}
		Set<String> dateResults = hostRegUids.get(date);
		Set<String> dateIMEIResults = hostRegIMEIs.get(date);
		if (dateResults == null) {
			dateResults = new HashSet<String>();
			dateIMEIResults = new HashSet<String>();
			// 从对应数据库中加载
			String tblName = platformID + "_log.tblAddPlayerLog_" + date.replace("-", "");
			List<String> options = new ArrayList<String>();
			options.add("HostID = '" + hostID + "'");
			options.add("Time >= '" + date + " 00:00:00'");
			options.add("Time <= '" + date + " 23:59:59'");
			Connection conn = DBManager.getConn();
			ResultSet resultSet = CommonDB.query(conn, tblName, options);
			try {
				while (resultSet.next()) {
					String uid = resultSet.getString("Uid");
					dateResults.add(uid);
					// 获得设备号
					String phoneInfo = resultSet.getString("PhoneInfo");
					String[] phoneInfos = StringUtils.splitPreserveAllTokens(phoneInfo, ";");
					if (phoneInfos.length >= 10) {
						String imei = phoneInfos[7];
						dateIMEIResults.add(imei);
					}
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				DBManager.closeConn(conn);
			}
			hostRegUids.put(date, dateResults);
			hostRegIMEIs.put(date, dateIMEIResults);
		}
		regMap.put("Uid", dateResults);
		regMap.put("IMEI", dateIMEIResults);
		return regMap;
	}

	/**
	 * 返回该服该天的首充玩家uid列表,如果该列表为none，则从数据库中加载数据，并缓存在RegResults中
	 * 
	 * @param platformID
	 * @param hostID
	 * @param date
	 * @return
	 */
	private Set<String> loadFirstPayUid(String platformID, String hostID, String date) {
		Map<String, Set<String>> hostFirstPayUids = FirstPayResults.get(hostID);
		if (hostFirstPayUids == null) {
			hostFirstPayUids = new HashMap<String, Set<String>>();
			FirstPayResults.put(hostID, hostFirstPayUids);
		}
		Set<String> dateResults = hostFirstPayUids.get(date);
		if (dateResults == null) {
			dateResults = new HashSet<String>();
			// 从对应数据库中加载
			String tblName = platformID + "_statics.tblUserPayStatics";
			List<String> options = new ArrayList<String>();
			options.add("HostID = '" + hostID + "'");
			options.add("FirstCashTime >= '" + date + " 00:00:00'");
			options.add("FirstCashTime <= '" + date + " 23:59:59'");
			Connection conn = DBManager.getConn();
			ResultSet resultSet = CommonDB.query(conn, tblName, options);
			try {
				while (resultSet.next()) {
					String uid = resultSet.getString("Uid");
					dateResults.add(uid);
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				DBManager.closeConn(conn);
			}
			hostFirstPayUids.put(date, dateResults);
		}
		return dateResults;
	}

	/**
	 * 统计登陆留存率
	 * 
	 * @param platformID
	 * @param hostID
	 * @param date
	 */
	private void staticsLoginRetention(String platformID, String hostID, String date) {
		// 获得当天的登陆玩家Uid
		Set<String> loginUids = LoginDayUids.get(hostID).get(date);
		int loginNum = loginUids.size();
		Map<String, Set<String>> regMap = loadRegMap(platformID, hostID, date);
		Set<String> dateRegUids = regMap.get("Uid");// 注册Uid
		int regNum = dateRegUids.size();
		// 先记录今天的注册数和登陆数
		Map<String, String> keyValues = new HashMap<String, String>();
		keyValues.put("NewNum", Integer.toString(regNum));
		keyValues.put("LoginNum", Integer.toString(loginNum));
		RetentionDB.insertLoginRetention(platformID, hostID, date, keyValues);
		// 再统计每天的留存率
		int[] days = { -1, -2, -3, -4, -5, -6, -7, -10, -13, -15, -29, -30 };
		for (int day : days) {
			String dayStr = DateUtils2.getOverDate(date, day);
			Map<String, Set<String>> dateRegMap = loadRegMap(platformID, hostID, dayStr);
			Set<String> regDay = dateRegMap.get("Uid");
			float rate = staticsRetentionRate(loginUids, regDay);
			Map<String, String> map = new HashMap<String, String>();
			String col = Math.abs(day) + "Days";
			map.put(col, Float.toString(rate));
			RetentionDB.insertLoginRetention(platformID, hostID, date, map);
		}
	}

	/**
	 * 统计付费留存率
	 * 
	 * @param platformID
	 * @param hostID
	 * @param date
	 */
	private void staticsPayUserRetention(String platformID, String hostID, String date) {
		// 获得当天的登陆玩家Uid
		Set<String> loginUids = LoginDayUids.get(hostID).get(date);
		int loginNum = loginUids.size();
		Set<String> dateRegUids = loadFirstPayUid(platformID, hostID, date);
		int regNum = dateRegUids.size();
		// 先记录今天的注册数和登陆数
		Map<String, String> keyValues = new HashMap<String, String>();
		keyValues.put("FirstPayUserNum", Integer.toString(regNum));
		keyValues.put("LoginNum", Integer.toString(loginNum));
		RetentionDB.insertPayRetention(platformID, hostID, date, keyValues);
		// 再统计每天的留存率
		int[] days = { -1, -2, -3, -4, -5, -6, -7, -10, -13, -15, -29, -30 };
		for (int day : days) {
			String dayStr = DateUtils2.getOverDate(date, day);
			Set<String> regDay = loadFirstPayUid(platformID, hostID, dayStr);
			float rate = staticsRetentionRate(loginUids, regDay);
			Map<String, String> map = new HashMap<String, String>();
			String col = Math.abs(day) + "Days";
			map.put(col, Float.toString(rate));
			RetentionDB.insertPayRetention(platformID, hostID, date, map);
		}
	}
	
	/**
	 * 统计登陆留存率
	 * 
	 * @param platformID
	 * @param hostID
	 * @param date
	 */
	private void staticsPhoneRetention(String platformID, String hostID, String date) {
		// 获得当天的登陆玩家Uid
		Set<String> loginUids = LoginDayIMEIs.get(hostID).get(date);
		int loginNum = loginUids.size();
		Map<String, Set<String>> regMap = loadRegMap(platformID, hostID, date);
		Set<String> dateRegUids = regMap.get("IMEI");// 注册Uid
		int regNum = dateRegUids.size();
		// 先记录今天的注册数和登陆数
		Map<String, String> keyValues = new HashMap<String, String>();
		keyValues.put("NewNum", Integer.toString(regNum));
		keyValues.put("LoginNum", Integer.toString(loginNum));
		RetentionDB.insertPhoneRetention(platformID, hostID, date, keyValues);
		// 再统计每天的留存率
		int[] days = { -1, -2, -3, -4, -5, -6, -7, -10, -13, -15, -29, -30 };
		for (int day : days) {
			String dayStr = DateUtils2.getOverDate(date, day);
			Map<String, Set<String>> dateRegMap = loadRegMap(platformID, hostID, dayStr);
			Set<String> regDay = dateRegMap.get("IMEI");
			float rate = staticsRetentionRate(loginUids, regDay);
			Map<String, String> map = new HashMap<String, String>();
			String col = Math.abs(day) + "Days";
			map.put(col, Float.toString(rate));
			RetentionDB.insertPhoneRetention(platformID, hostID, date, map);
		}
	}

	/**
	 * 根据登陆玩家Uid和注册玩家uid计算留存率
	 * 
	 * @param loginUids
	 * @param regUids
	 * @return
	 */
	public float staticsRetentionRate(Set<String> loginUids, Set<String> regUids) {
		float rate = 0;
		float regNum = regUids.size();
		int loginNum = loginUids.size();
		if (loginNum > 0 && regNum > 0) {
			int num = 0;
			for (String uid : loginUids) {
				if (regUids.contains(uid)) {
					num++;
				}
			}
			float bench = 100;
			rate = Math.round(num * 10000 / regNum) / bench;
		}
		return rate;
	}

	/**
	 * 获得某天的注册玩家uid列表
	 * 
	 * @param hostID
	 * @param date
	 * @return
	 */
	public static Set<String> getRegUids(String platformID, String hostID, String date) {
		Map<String, Set<String>> regMap = loadRegMap(platformID, hostID, date);
		return regMap.get("Uid");
	}

	/**
	 * 获得某天的登陆玩家uid列表
	 * 
	 * @param hostID
	 * @param date
	 * @return
	 */
	public static Set<String> getLoginUids(String platformID, String hostID, String date) {
		Map<String, Set<String>> hostUids = LoginDayUids.get(hostID);
		if (hostUids == null) {
			hostUids = new HashMap<String, Set<String>>();
			LoginDayUids.put(hostID, hostUids);
		}
		Set<String> loginUids = hostUids.get(date);
		if (loginUids == null) {
			Map<String, Set<String>> loginData = loadLoginUidFromDB(platformID, hostID, date);
			loginUids = loginData.get("Uid");
			Map<String, Set<String>> logoutData = loadLogoutUidFromDB(platformID, hostID, date);
			Set<String> logoutUids = logoutData.get("Uid");
			loginUids.addAll(logoutUids);
			hostUids.put(date, loginUids);
		}
		return loginUids;
	}

	@Override
	public boolean cronExecute() {
		synchronized (PeriodLoginUids) {
			Iterator<String> pIt = PeriodLoginUids.keySet().iterator();
			while (pIt.hasNext()) {
				String platformID = pIt.next();
				Iterator<String> hIt = PeriodLoginUids.get(platformID).keySet().iterator();
				while (hIt.hasNext()) {
					String hostID = hIt.next();
					Iterator<String> dIt = PeriodLoginUids.get(platformID).get(hostID).keySet().iterator();
					while (dIt.hasNext()) {
						String date = dIt.next();
						staticsLoginRetention(platformID, hostID, date); // 登陆留存
						staticsPayUserRetention(platformID, hostID, date);// 付费留存
						staticsPhoneRetention(platformID, hostID, date); // 设备留存
					}
				}
			}
			// 其他没有数据的服也要统计
			String today = DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd");
			Map<String, String> hostMap = ServerDB.getStaticsServers();
			Iterator<String> hIt = hostMap.keySet().iterator();
			while (hIt.hasNext()) {
				String hostID = hIt.next();
				String platformID = hostMap.get(hostID);
				if (!LoginDayUids.containsKey(hostID) || !LoginDayUids.get(hostID).containsKey(today)) {
					Map<String, Set<String>> todayUids = LoginDayUids.get(hostID);
					Map<String, Set<String>> todayIMEIs = null;
					if (todayUids == null) {
						todayUids = new HashMap<String, Set<String>>();
						LoginDayUids.put(hostID, todayUids);
						todayIMEIs = new HashMap<String, Set<String>>();
						LoginDayIMEIs.put(hostID, todayIMEIs);
					}
					Set<String> uids = todayUids.get(today);
					if (uids == null) {
						Map<String, Set<String>> loginData = loadLoginUidFromDB(platformID, hostID, today);
						uids = loginData.get("Uid");
						Map<String, Set<String>> logoutData = loadLogoutUidFromDB(platformID, hostID, today);
						Set<String> logoutUids = logoutData.get("Uid");
						uids.addAll(logoutUids);
						todayUids.put(today, uids);
						//还要累加IMEI设备号，便于统计设备留存
						Set<String> imeis = loginData.get("IMEI");
						imeis.addAll(logoutData.get("IMEI"));
						todayIMEIs.put(today, imeis);
						staticsLoginRetention(platformID, hostID, today); // 登陆留存
						staticsPayUserRetention(platformID, hostID, today);// 付费留存
						staticsPhoneRetention(platformID, hostID, today); // 设备留存
					}
				}
			}
			// 清空
			if (PeriodLoginUids.size() > 0){
				PeriodLoginUids = new HashMap<String, Map<String, Map<String, Set<String>>>>();
				PeriodLoginIMEIs = new HashMap<String, Map<String, Map<String, Set<String>>>>();
			}
				
			
		}
		return true;
	}

}
