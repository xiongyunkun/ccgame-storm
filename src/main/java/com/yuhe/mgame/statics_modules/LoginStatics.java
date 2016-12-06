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
import com.yuhe.mgame.db.statics.IMEIInfoDB;
import com.yuhe.mgame.db.statics.LoginStaticsDB;
import com.yuhe.mgame.db.statics.StartNumDB;

/**
 * 登陆过程分析统计
 * 
 * @author xiongyunkun
 *
 */
public class LoginStatics extends AbstractStaticsModule {
	private static final int STANDARD_ID = 15; // 标准ID，如果不在这个步骤里面的不用统计
	private static final int IMEI_STANDARD_ID = 1; // 设备登陆标准ID，如果不在这个步骤里面的就不用统计
	private static final int W3_HOSTID = 666; // 3w的日志记录的hostID
	// 记录当天登陆过程的玩家uid，不在这里面的不予统计，另外时间过了还需要及时清空,格式:<HostID, <date, <Uids>>>
	private Map<String, Map<String, Set<String>>> StandardUids = new HashMap<String, Map<String, Set<String>>>();
	// 记录需要入库的统计结果，入库完毕后这些统计结果会被清空，格式：// 记录格式<platformID, HostID,<Date, <Hour,
	// <Step, StepNum>>>>>
	private Map<String, Map<String, Map<String, Map<String, Map<String, Integer>>>>> PlatformStatics = new HashMap<String, Map<String, Map<String, Map<String, Map<String, Integer>>>>>();
	// 记录已经统计过的设备号与流程ID的关系，用于排重，下次不要再进行统计,记录格式:<PlatformID, <IMEI, Step>>
	private Map<String, Map<String, Set<String>>> OldIMEIMap = new HashMap<String, Map<String, Set<String>>>();
	// 记录当天登陆过的设备号ID，不在这里的不予统计，另外过了时间的还要及时清空,记录格式:<PlatformID, <Date, <IMEI>>>
	private Map<String, Map<String, Set<String>>> StandardIMEIs = new HashMap<String, Map<String, Set<String>>>();
	// 记录需要入库的IMEI统计结果，入库完毕后这些统计会被清空，格式：<platformID, <Date, <Hour, <Step,
	// StepNum>>>>
	private Map<String, Map<String, Map<String, Map<String, Integer>>>> PlatformIMEIStatics = new HashMap<String, Map<String, Map<String, Map<String, Integer>>>>();

	@Override
	public synchronized boolean execute(Map<String, List<Map<String, String>>> platformResults) {
		Iterator<String> pIt = platformResults.keySet().iterator();
		while (pIt.hasNext()) {
			String platformID = pIt.next();
			List<Map<String, String>> platformResult = platformResults.get(platformID);
			Map<String, Map<String, Map<String, Map<String, Integer>>>> hostResults = PlatformStatics.get(platformID);
			Map<String, Map<String, Map<String, Integer>>> imeiStatics = PlatformIMEIStatics.get(platformID);
			if (hostResults == null) {
				hostResults = new HashMap<String, Map<String, Map<String, Map<String, Integer>>>>();
				PlatformStatics.put(platformID, hostResults);
				imeiStatics = new HashMap<String, Map<String, Map<String, Integer>>>();
				PlatformIMEIStatics.put(platformID, imeiStatics);
			}
			List<Map<String, String>> newIMEIList = new ArrayList<Map<String, String>>();
			for (Map<String, String> logInfo : platformResult) {
				String hostID = logInfo.get("HostID");
				if (Integer.valueOf(hostID) == W3_HOSTID) {
					Map<String, String> imeiInfo = mergeLoginIMEIs(platformID, logInfo, imeiStatics);
					if(imeiInfo.size() > 0)
						newIMEIList.add(imeiInfo);
				} else {
					mergeLoginUids(platformID, logInfo, hostResults);
				}
			}
			//如果是设备流程的日志还需要更新tblIMEIInfo表中的设备号和登陆步骤ID
			if(newIMEIList.size() > 0){
				IMEIInfoDB.insert(platformID, newIMEIList);
				StartNumDB.insert(platformID, newIMEIList);
			}
		}
		return true;
	}

	/**
	 * 合并登陆uid
	 * 
	 * @param platformID
	 * @param logInfo
	 * @param hostResults
	 */
	public void mergeLoginUids(String platformID, Map<String, String> logInfo,
			Map<String, Map<String, Map<String, Map<String, Integer>>>> hostResults) {
		String hostID = logInfo.get("HostID");
		String uid = logInfo.get("Uid");
		String time = logInfo.get("Time");
		String step = logInfo.get("Step");

		String[] timeInfo = getTimeInfo(time);
		String date = timeInfo[0];
		String hour = timeInfo[1];
		if (date != null && hour != null) {
			// 分类整理
			Set<String> standardUids = getStandardUids(platformID, hostID, date);
			if (Integer.parseInt(step) == STANDARD_ID)
				standardUids.add(uid);
			// 前面登陆验证的步骤都需要统计进去
			if (standardUids.contains(uid)) {
				// 只统计今天登陆游戏的，之前登陆的都不予计算
				Map<String, Map<String, Map<String, Integer>>> hostResult = hostResults.get(hostID);
				if (hostResult == null) {
					hostResult = new HashMap<String, Map<String, Map<String, Integer>>>();
					hostResults.put(hostID, hostResult);
				}
				Map<String, Map<String, Integer>> dateResult = hostResult.get(date);
				if (dateResult == null) {
					dateResult = new HashMap<String, Map<String, Integer>>();
					hostResult.put(date, dateResult);
				}
				Map<String, Integer> hourResult = dateResult.get(hour);
				if (hourResult == null) {
					hourResult = new HashMap<String, Integer>();
					dateResult.put(hour, hourResult);
				}
				int stepNum = hourResult.getOrDefault(step, 0);
				hourResult.put(step, stepNum + 1);
			}
		}
	}

	/**
	 * 合并登陆设备号,返回新增的设备号以及登陆步骤
	 * 
	 * @param platformID
	 * @param logInfo
	 * @param imeiStatics
	 */
	public Map<String, String> mergeLoginIMEIs(String platformID, Map<String, String> logInfo,
			Map<String, Map<String, Map<String, Integer>>> imeiStatics) {
		Map<String, String> imeiInfo = new HashMap<String, String>();
		String time = logInfo.get("Time");
		String step = logInfo.get("Step");
		String imei = logInfo.get("IMEI");
		String phoneInfo = logInfo.get("PhoneInfo");
		String[] timeInfo = getTimeInfo(time);
		String date = timeInfo[0];
		String hour = timeInfo[1];
		if (date != null && hour != null) {
			// 分类整理
			Set<String> standardIMEIs = getStandardIMEIs(platformID, date);
			if (Integer.parseInt(step) == IMEI_STANDARD_ID){
				standardIMEIs.add(imei);
			}
			if (standardIMEIs.contains(imei)) {
				Map<String, Set<String>> oldIMEIs = getOldIMEIMap(platformID);
				if (!oldIMEIs.containsKey(imei) || !oldIMEIs.get(imei).contains(step)) {
					Map<String, Map<String, Integer>> dateResult = imeiStatics.get(date);
					if (dateResult == null) {
						dateResult = new HashMap<String, Map<String, Integer>>();
						imeiStatics.put(date, dateResult);
					}
					Map<String, Integer> hourResult = dateResult.get(hour);
					if (hourResult == null) {
						hourResult = new HashMap<String, Integer>();
						dateResult.put(hour, hourResult);
					}
					int stepNum = hourResult.getOrDefault(step, 0);
					hourResult.put(step, stepNum + 1);
					//同时还要更新OldIMEIMap表便于下次排重
					Set<String> stepSet = oldIMEIs.get(imei);
					if(stepSet == null){
						stepSet = new HashSet<String>();
						oldIMEIs.put(imei, stepSet);
					}
					stepSet.add(step);
					imeiInfo.put("IMEI", imei);
					imeiInfo.put("Step", step);
					imeiInfo.put("Time", time);
					if (Integer.parseInt(step) == IMEI_STANDARD_ID){
						//如果是登陆第一步还需要记录DPI，Model， Brand
						String[] phoneInfos = StringUtils.split(phoneInfo, ";");
						if(phoneInfos.length >= 7){
							imeiInfo.put("Model", phoneInfos[0]); //Model
							imeiInfo.put("DPI", phoneInfos[5] + "*" + phoneInfos[6]);
							imeiInfo.put("Brand", phoneInfos[1]);
							imeiInfo.put("Date", date);
						}
					}
				}
			}
		}
		return imeiInfo;
	}

	/**
	 * 获得今天登陆过游戏的玩家Uid
	 * 
	 * @param platformID
	 * @param hostID
	 * @param date
	 * @return
	 */
	private Set<String> getStandardUids(String platformID, String hostID, String date) {
		Map<String, Set<String>> hostUids = StandardUids.get(hostID);
		if (hostUids == null) {
			hostUids = new HashMap<String, Set<String>>();
			StandardUids.put(hostID, hostUids);
		}
		Set<String> uids = hostUids.get(date);
		if (uids == null) {
			uids = loadUidFromDB(platformID, hostID, date);
			hostUids.put(date, uids);
			// 新增这一天的同时需要把其他天数的数据清空
			Iterator<String> it = hostUids.keySet().iterator();
			while (it.hasNext()) {
				String tDate = it.next();
				if (!tDate.equals(date)) {
					it.remove();
				}
			}
		}
		return uids;
	}

	/**
	 * 从数据库中加载Uid
	 * 
	 * @param platformID
	 * @param hostID
	 * @param date
	 * @return
	 */
	private Set<String> loadUidFromDB(String platformID, String hostID, String date) {
		Set<String> uids = new HashSet<String>();
		String tblName = platformID + "_log.tblClientLoadLog_" + date.replace("-", "");
		List<String> options = new ArrayList<String>();
		options.add("HostID = '" + hostID + "'");
		options.add("Time >= '" + date + " 00:00:00'");
		options.add("Time <= '" + date + " 23:59:59'");
		options.add("Step = '" + STANDARD_ID + "'");
		Connection conn = DBManager.getConn();
		ResultSet resultSet = CommonDB.query(conn, tblName, options);
		try {
			while (resultSet.next()) {
				String uid = resultSet.getString("Uid");
				uids.add(uid);
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
	 * 从时间中获得日期和小时
	 * 
	 * @param time
	 * @return
	 */
	private String[] getTimeInfo(String time) {
		String[] timeInfo = new String[2];
		String[] times = StringUtils.split(time, " ");
		if (times.length >= 2) {
			timeInfo[0] = times[0]; // 日期
			String[] hourInfo = StringUtils.split(times[1], ":");
			if (hourInfo.length >= 3) {
				timeInfo[1] = hourInfo[0];
			}
		}
		return timeInfo;
	}

	/**
	 * 获得已经统计过的设备号与登陆流程ID的对应map
	 * 
	 * @param platformID
	 * @return
	 */
	private Map<String, Set<String>> getOldIMEIMap(String platformID) {
		Map<String, Set<String>> imeiMap = OldIMEIMap.get(platformID);
		if (imeiMap == null) {
			imeiMap = loadIMEIMapFromDB(platformID);
			OldIMEIMap.put(platformID, imeiMap);
		}
		return imeiMap;
	}

	/**
	 * 从数据库tblIMEIInfo表中加载已经记录的设备号和登陆步骤的关系，用于设备流程排重
	 * 
	 * @param platformID
	 * @return
	 */
	private Map<String, Set<String>> loadIMEIMapFromDB(String platformID) {
		Map<String, Set<String>> imeiMap = new HashMap<String, Set<String>>();
		String tblName = platformID + "_statics.tblIMEIInfo";
		List<String> options = new ArrayList<String>();
		Connection conn = DBManager.getConn();
		ResultSet resultSet = CommonDB.query(conn, tblName, options);
		try {
			while (resultSet.next()) {
				String imei = resultSet.getString("IMEI");
				String step = resultSet.getString("Step");
				Set<String> stepSet = imeiMap.get(imei);
				if (stepSet == null) {
					stepSet = new HashSet<String>();
					imeiMap.put(imei, stepSet);
				}
				stepSet.add(step);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			DBManager.closeConn(conn);
		}
		return imeiMap;
	}

	/**
	 * 获得标准设备号IMEI列表
	 * 
	 * @param platformID
	 * @param date
	 * @return
	 */
	private Set<String> getStandardIMEIs(String platformID, String date) {
		Map<String, Set<String>> platformMap = StandardIMEIs.get(platformID);
		if (platformMap == null) {
			platformMap = new HashMap<String, Set<String>>();
			StandardIMEIs.put(platformID, platformMap);
		}
		Set<String> imeiSet = platformMap.get(date);
		if (imeiSet == null) {
			imeiSet = loadIMEIFromDB(platformID, date);
			platformMap.put(date, imeiSet);
			// 需要把其他日期的设备号数据都清空
			Iterator<String> it = platformMap.keySet().iterator();
			while (it.hasNext()) {
				String tDate = it.next();
				if (!tDate.equals(date)) {
					it.remove();
				}
			}
		}
		return imeiSet;
	}

	/**
	 * 从数据库中加载设备号
	 * 
	 * @param platformID
	 * @param date
	 * @return
	 */
	private Set<String> loadIMEIFromDB(String platformID, String date) {
		Set<String> imeiSet = new HashSet<String>();
		String tblName = platformID + "_log.tblClientLoadLog_" + date.replace("-", "");
		List<String> options = new ArrayList<String>();
		options.add("HostID = '" + W3_HOSTID + "'");
		options.add("Time >= '" + date + " 00:00:00'");
		options.add("Time <= '" + date + " 23:59:59'");
		options.add("Step = '" + STANDARD_ID + "'");
		Connection conn = DBManager.getConn();
		ResultSet resultSet = CommonDB.query(conn, tblName, options);
		try {
			while (resultSet.next()) {
				String imei = resultSet.getString("IMEI");
				imeiSet.add(imei);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			DBManager.closeConn(conn);
		}
		return imeiSet;
	}

	/**
	 * 定时将统计结果写入数据库
	 */
	@Override
	public synchronized boolean cronExecute() {
		cronUidStatics();
		cronIMEIStatics();
		return true;
	}

	private boolean cronUidStatics() {
		Iterator<String> pIt = PlatformStatics.keySet().iterator();
		while (pIt.hasNext()) {
			String platformID = pIt.next();
			Map<String, Map<String, Map<String, Map<String, Integer>>>> hostResults = PlatformStatics.get(platformID);
			// 再按照小时重新合并整理数据
			Iterator<String> hIt = hostResults.keySet().iterator();
			while (hIt.hasNext()) {
				String hostID = hIt.next();
				Map<String, Map<String, Map<String, Integer>>> hostResult = hostResults.get(hostID);
				Iterator<String> dIt = hostResult.keySet().iterator();
				while (dIt.hasNext()) {
					String date = dIt.next();
					Map<String, Map<String, Integer>> dateResult = hostResult.get(date);
					Iterator<String> hourIt = dateResult.keySet().iterator();
					while (hourIt.hasNext()) {
						String hour = hourIt.next();
						Map<String, Integer> hourResult = dateResult.get(hour);
						Map<String, Integer> stepResult = new HashMap<String, Integer>();

						Iterator<String> sIt = hourResult.keySet().iterator();
						while (sIt.hasNext()) {
							String step = sIt.next();
							stepResult.put(step, hourResult.get(step));
						}
						// 记录数据库
						if (stepResult.size() > 0) {
							LoginStaticsDB.insert(platformID, hostID, date, hour, stepResult);
						}
					}
				}
			}
		}
		// 记录入库后重新清空
		if (PlatformStatics.size() > 0) {
			PlatformStatics = new HashMap<String, Map<String, Map<String, Map<String, Map<String, Integer>>>>>();
		}
		return true;
	}

	/**
	 * 统计设备流程分析
	 * 
	 * @return
	 */
	private boolean cronIMEIStatics() {
		Iterator<String> pIt = PlatformIMEIStatics.keySet().iterator();
		while (pIt.hasNext()) {
			String platformID = pIt.next();
			Map<String, Map<String, Map<String, Integer>>> hostResult = PlatformIMEIStatics.get(platformID);
			Iterator<String> dIt = hostResult.keySet().iterator();
			while (dIt.hasNext()) {
				String date = dIt.next();
				Map<String, Map<String, Integer>> dateResult = hostResult.get(date);
				Iterator<String> hourIt = dateResult.keySet().iterator();
				while (hourIt.hasNext()) {
					String hour = hourIt.next();
					Map<String, Integer> hourResult = dateResult.get(hour);
					Map<String, Integer> stepResult = new HashMap<String, Integer>();

					Iterator<String> sIt = hourResult.keySet().iterator();
					while (sIt.hasNext()) {
						String step = sIt.next();
						stepResult.put(step, hourResult.get(step));
					}
					// 记录数据库
					if (stepResult.size() > 0) {
						LoginStaticsDB.insert(platformID, String.valueOf(W3_HOSTID), date, hour, stepResult);
					}
				}
			}
		}
		// 记录入库后重新清空
		if (PlatformIMEIStatics.size() > 0) {
			PlatformIMEIStatics = new HashMap<String, Map<String, Map<String, Map<String, Integer>>>>();
		}
		return true;
	}

}
