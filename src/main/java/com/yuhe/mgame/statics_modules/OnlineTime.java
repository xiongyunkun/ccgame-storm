package com.yuhe.mgame.statics_modules;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.yuhe.mgame.db.DBManager;
import com.yuhe.mgame.db.log.CommonDB;
import com.yuhe.mgame.db.statics.OnlineTimeDB;

/**
 * 统计在线时长
 * 
 * @author xiongyunkun
 *
 */
public class OnlineTime extends AbstractStaticsModule {
	// 记录玩家的在线时长信息，格式:Map<HostID, Map<Date,Map<Uid, OnlineTime>>>
	private Map<String, Map<String, Map<String, Integer>>> PlayerOnlineTimes = new HashMap<String, Map<String, Map<String, Integer>>>();
	// 记录在线时间段信息,格式:Map<HostID, Map<Date, Map<UserType,
	// Map<ZoneID,OnlineTime>>>>
	private Map<String, Map<String, Map<Integer, Map<String, Integer>>>> OnlineTimeZones = new HashMap<String, Map<String, Map<Integer, Map<String, Integer>>>>();
	// 在线时长区间ID
	private static final Map<String, int[]> ZONE_MAP = new HashMap<String, int[]>() {
		private static final long serialVersionUID = 1L;

		{
			put("Time0", new int[] { 0, 1 });
			put("Time1", new int[] { 1, 5 });
			put("Time5", new int[] { 5, 10 });
			put("Time10", new int[] { 10, 15 });
			put("Time15", new int[] { 15, 30 });
			put("Time30", new int[] { 30, 45 });
			put("Time45", new int[] { 45, 60 });
			put("Time60", new int[] { 60, 90 });
			put("Time90", new int[] { 90, 120 });
			put("Time120", new int[] { 120, 150 });
			put("Time150", new int[] { 150, 180 });
			put("Time180", new int[] { 180, 240 });
			put("Time240", new int[] { 240, 300 });
			put("Time300", new int[] { 300, 360 });
			put("Time360", new int[] { 360, 420 });
			put("Time420", new int[] { 420, 600 });
			put("Time600", new int[] { 600, 900 });
			put("Time900", new int[] { 900, 1200 });
			put("Time1200", new int[] { 1200 });
		}

	};

	@Override
	public boolean execute(Map<String, List<Map<String, String>>> platformResults) {
		boolean flag = false;
		Map<String, String> hostMap = new HashMap<String, String>();
		Iterator<String> pIt = platformResults.keySet().iterator();
		while (pIt.hasNext()) {
			String platformID = pIt.next();
			List<Map<String, String>> platformResult = platformResults.get(platformID);
			for (Map<String, String> logMap : platformResult) {
				if (logMap.containsKey("OnTime")) {
					flag = true;
					String hostID = logMap.get("HostID");
					hostMap.put(hostID, platformID);
					String uid = logMap.get("Uid");
					String onlineTime = logMap.get("OnTime");
					String time = logMap.get("Time");
					String[] times = StringUtils.split(time, " ");
					String date = times[0];
					addOnlineTime(platformID, hostID, date, uid, Integer.parseInt(onlineTime));
				}
			}
		}
		if(flag == true){
			// 记录更新数据库
			Iterator<String> hIt = hostMap.keySet().iterator();
			while (hIt.hasNext()) {
				String hostID = hIt.next();
				String platformID = hostMap.get(hostID);
				Map<String, Map<Integer, Map<String, Integer>>> dateResults = OnlineTimeZones.get(hostID);
				Iterator<String> dIt = dateResults.keySet().iterator();
				while (dIt.hasNext()) {
					String date = dIt.next();
					Map<Integer, Map<String, Integer>> userResults = dateResults.get(date);
					Iterator<Integer> uIt = userResults.keySet().iterator();
					while (uIt.hasNext()) {
						int userType = uIt.next();
						Map<String, Integer> timeMap = userResults.get(userType);
						// 计算总登陆人数
						Set<String> loginUids = Retention.getLoginUids(platformID, hostID, date);
						OnlineTimeDB.insert(platformID, hostID, date, userType, loginUids.size(), timeMap);
					}
				}
			}
		}
		return true;
	}

	/**
	 * 累加玩家当天的在线时长，同时清空昨天之前的数据
	 * 
	 * @param platformID
	 * @param hostID
	 * @param date
	 * @param uid
	 * @param onlineTime
	 */
	private void addOnlineTime(String platformID, String hostID, String date, String uid, int onlineTime) {
		Map<String, Map<String, Integer>> hostResults = PlayerOnlineTimes.get(hostID);
		if (hostResults == null) {
			// 连host什么数据都没有的话就需要从数据库中去加载了
			hostResults = new HashMap<String, Map<String, Integer>>();
			Map<String, Integer> orgOnlineTimes = loadOnlineTimeFromDB(platformID, hostID, date);
			hostResults.put(date, orgOnlineTimes);
			PlayerOnlineTimes.put(hostID, hostResults);
		}
		Map<String, Integer> dateResults = hostResults.get(date);
		if (dateResults == null) {
			dateResults = new HashMap<String, Integer>();
			hostResults.put(date, dateResults);
		}
		// 这里还要删除除了今天之外的其他天的数据
		if (hostResults.size() > 1) {
			Iterator<String> dIt = hostResults.keySet().iterator();
			while (dIt.hasNext()) {
				String tDate = dIt.next();
				if (!tDate.equals(date)) {
					dIt.remove();
				}
			}
		}
		// 累加时间
		int orgTime = dateResults.getOrDefault(uid, 0);
		int totalTime = orgTime + onlineTime;
		dateResults.put(uid, totalTime);
		Set<String> regUids = Retention.getRegUids(platformID, hostID, date);
		int userType = 1; // 默认都是老用户
		if (regUids.contains(uid)) {
			userType = 2;
		}
		// 更新时长统计区间
		updateZoneTimes(hostID, date, userType, orgTime, totalTime);
	}

	/**
	 * 从登出日志中获取玩家当天的在线时长
	 * 
	 * @param platformID
	 * @param hostID
	 * @param date
	 * @return
	 */
	private Map<String, Integer> loadOnlineTimeFromDB(String platformID, String hostID, String date) {
		Map<String, Integer> onlineTimes = new HashMap<String, Integer>();
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
				int onTime = resultSet.getInt("OnTime");
				int orgOnTime = onlineTimes.getOrDefault(uid, 0);
				int totalOnTime = onTime + orgOnTime;
				onlineTimes.put(uid, totalOnTime);
				Set<String> regUids = Retention.getRegUids(platformID, hostID, date);
				int userType = 1; // 默认都是老用户
				if (regUids.contains(uid)) {
					userType = 2;
				}
				// 这里也要更新时长统计区间
				updateZoneTimes(hostID, date, userType, orgOnTime, totalOnTime);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			DBManager.closeConn(conn);
		}
		return onlineTimes;
	}

	/**
	 * 更新在线时间段信息
	 * 
	 * @param hostID
	 * @param date
	 * @param userType
	 * @param orgTime
	 * @param nowTime
	 */
	private void updateZoneTimes(String hostID, String date, int userType, int orgTime, int nowTime) {
		Map<String, Map<Integer, Map<String, Integer>>> hostResults = OnlineTimeZones.get(hostID);
		if (hostResults == null) {
			hostResults = new HashMap<String, Map<Integer, Map<String, Integer>>>();
			OnlineTimeZones.put(hostID, hostResults);
		}
		Map<Integer, Map<String, Integer>> dateResults = hostResults.get(date);
		if (dateResults == null) {
			dateResults = new HashMap<Integer, Map<String, Integer>>();
			hostResults.put(date, dateResults);
			if (hostResults.size() > 1) {
				// 其他天数的数据都删除
				Iterator<String> dIt = hostResults.keySet().iterator();
				while (dIt.hasNext()) {
					String tDate = dIt.next();
					if (!tDate.equals(date)) {
						dIt.remove();
					}
				}
			}
		}
		Map<String, Integer> typeResults = dateResults.get(userType);
		if (typeResults == null) {
			typeResults = new HashMap<String, Integer>();
			dateResults.put(userType, typeResults);
		}
		String orgZoneID = getZoneID(orgTime);
		String nowZoneID = getZoneID(nowTime);
		// 原有的区间数量要减1
		int orgNum = typeResults.getOrDefault(orgZoneID, 0);
		typeResults.put(orgZoneID, Math.max(orgNum - 1, 0));
		// 现有的区间数量要加1
		int nowNum = typeResults.getOrDefault(nowZoneID, 0);
		typeResults.put(nowZoneID, nowNum + 1);
		// 记录总在线时间,只统计在线时间超过5分钟的玩家的在线时长
		if (nowTime >= 300) {
			int totalTimes = typeResults.getOrDefault("TotalTimes", 0);
			totalTimes += nowTime - orgTime;
			typeResults.put("TotalTimes", totalTimes);
		}
	}

	/**
	 * 根据在线时长获得所在时长的区间ID
	 * 
	 * @param onlineTime
	 * @return
	 */
	private String getZoneID(int onlineTime) {
		String zoneID = null;
		Iterator<String> it = ZONE_MAP.keySet().iterator();
		while (it.hasNext()) {
			String tZoneID = it.next();
			int[] zones = ZONE_MAP.get(tZoneID);
			if (zones.length == 2 && onlineTime >= (zones[0] * 60) && onlineTime < (zones[1] * 60)) {
				zoneID = tZoneID;
				break;
			}
			if (zones.length == 1 && onlineTime >= zones[0] * 60) {
				zoneID = tZoneID;
				break;
			}

		}
		return zoneID;
	}

	@Override
	public boolean cronExecute() {
		// TODO Auto-generated method stub
		return false;
	}

}
