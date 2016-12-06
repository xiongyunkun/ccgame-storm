package com.yuhe.mgame.statics_modules;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
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
import com.yuhe.mgame.db.statics.HistoryOnlineDB;


public class HistoryOnline extends AbstractStaticsModule {
	// 记录当天服的人数信息：最高在线，最低在线，总在线人数
	// 数据格式：<HostID, <date, <Type, Number>>>
	private Map<String, Map<String, Map<String, Integer>>> StaticsNumMap = new HashMap<String, Map<String, Map<String, Integer>>>();

	@Override
	public boolean execute(Map<String, List<Map<String, String>>> platformResults) {
		Set<String> flagSet = new HashSet<String>(); // 标志位，用来记录到底哪些hostid哪些date需要更新
		Iterator<String> pIt = platformResults.keySet().iterator();
		while (pIt.hasNext()) {
			String platformID = pIt.next();
			List<Map<String, String>> platformResult = platformResults.get(platformID);
			for (Map<String, String> logMap : platformResult) {
				String hostID = logMap.get("HostID");
				String onlineNumStr = logMap.get("OnlineNum");
				int onlineNum = Integer.parseInt(onlineNumStr);
				String time = logMap.get("Time");
				String[] times = StringUtils.split(time, " ");
				String date = times[0];
				Map<String, Map<String, Integer>> hostResults = StaticsNumMap.get(hostID);
				if (hostResults == null) {
					hostResults = loadFromDB(platformID, hostID, date);
					StaticsNumMap.put(hostID, hostResults);
				}
				Map<String, Integer> dateResult = hostResults.get(date);
				if (dateResult == null) {
					dateResult = new HashMap<String, Integer>();
					dateResult.put("MaxOnline", 0);
					dateResult.put("MinOnline", 0);
					dateResult.put("TotalOnline", 0);
					hostResults = new HashMap<String, Map<String, Integer>>(); // 其他天数的数据都要清除
					hostResults.put(date, dateResult);
				}
				int maxOnline = dateResult.get("MaxOnline");
				int minOnline = dateResult.get("MinOnline");
				int totalOnline = dateResult.get("TotalOnline");
				if (maxOnline < onlineNum)
					maxOnline = onlineNum;
				if (minOnline == 0 || minOnline > onlineNum)
					minOnline = onlineNum;
				totalOnline += onlineNum;
				dateResult.put("MaxOnline", maxOnline);
				dateResult.put("MinOnline", minOnline);
				dateResult.put("TotalOnline", totalOnline);
				flagSet.add(platformID + ";" + hostID + ";" + date);
			}
		}
		// 记录数据库
		for (String str : flagSet) {
			String[] strs = StringUtils.split(str, ";");
			if (strs.length == 3) {
				Map<String, Integer> numMap = StaticsNumMap.get(strs[1]).get(strs[2]);
				int totalOnline = numMap.get("TotalOnline");
				int maxOnline = numMap.get("MaxOnline");
				int minOnline = numMap.get("MinOnline");
				long period = getPeriod();
				int aveNum = (int) Math.floorDiv(totalOnline, period);
				HistoryOnlineDB.batchInsert(strs[0], strs[1], strs[2], maxOnline, aveNum, minOnline);
			}
		}
		return true;
	}

	/**
	 * 从tblOnline表中获得当天的最大在线，最小在线，总在线人数
	 * 
	 * @param platformID
	 * @param hostID
	 * @param date
	 * @return
	 */
	private Map<String, Map<String, Integer>> loadFromDB(String platformID, String hostID, String date) {
		Map<String, Map<String, Integer>> hostResults = new HashMap<String, Map<String, Integer>>();
		Map<String, Integer> dateResult = new HashMap<String, Integer>();
		dateResult.put("MaxOnline", 0);
		dateResult.put("MinOnline", 0);
		dateResult.put("TotalOnline", 0);
		String tblName = platformID + "_statics.tblOnline";
		List<String> options = new ArrayList<String>();
		options.add("HostID = '" + hostID + "'");
		options.add("Time >= '" + date + " 00:00:00'");
		options.add("Time <= '" + date + " 23:59:59'");
		Connection conn = DBManager.getConn();
		ResultSet resultSet = CommonDB.query(conn, tblName, options);
		try {
			while (resultSet.next()) {
				int onlineNum = resultSet.getInt("OnlineNum");
				int maxOnline = dateResult.get("MaxOnline");
				int minOnline = dateResult.get("MinOnline");
				int totalOnline = dateResult.get("TotalOnline");
				if (maxOnline < onlineNum)
					maxOnline = onlineNum;
				if (minOnline == 0 || minOnline > onlineNum)
					minOnline = onlineNum;
				totalOnline += onlineNum;
				dateResult.put("MaxOnline", maxOnline);
				dateResult.put("MinOnline", minOnline);
				dateResult.put("TotalOnline", totalOnline);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			DBManager.closeConn(conn);
		}
		hostResults.put(date, dateResult);
		return hostResults;
	}

	/**
	 * 计算从0点开始到当前时间已经过去了多少个5分钟
	 * 
	 * @return
	 */
	private long getPeriod() {
		Calendar benCal = Calendar.getInstance();
		benCal.set(Calendar.HOUR_OF_DAY, 0);
		benCal.set(Calendar.SECOND, 0);
		benCal.set(Calendar.MINUTE, 0);
		benCal.set(Calendar.MILLISECOND, 0);
		long benTime = benCal.getTimeInMillis();
		// 当前时间戳
		Calendar cal = Calendar.getInstance();
		long nowTime = cal.getTimeInMillis();
		long diff = nowTime - benTime;
		long floor = Math.floorDiv(diff, 300000);
		return floor;
	}

	@Override
	public boolean cronExecute() {
		synchronized (StaticsNumMap) {
			String today = DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd");
			Map<String, String> hostMap = ServerDB.getStaticsServers();
			Iterator<String> hIt = hostMap.keySet().iterator();
			while (hIt.hasNext()) {
				String hostID = hIt.next();
				String platformID = hostMap.get(hostID);
				Map<String, Map<String, Integer>> hostResult = StaticsNumMap.get(hostID);
				if (hostResult == null) {
					hostResult = new HashMap<String, Map<String, Integer>>();
					StaticsNumMap.put(hostID, hostResult);
				}
				Map<String, Integer> dateResult = hostResult.get(today);
				if (dateResult == null) {
					hostResult = loadFromDB(platformID, hostID, today);
					dateResult = hostResult.get(today);
					hostResult.put(today, dateResult);
					int totalOnline = dateResult.get("TotalOnline");
					int maxOnline = dateResult.get("MaxOnline");
					int minOnline = dateResult.get("MinOnline");
					long period = getPeriod();
					int aveNum = (int) Math.floorDiv(totalOnline, period);
					HistoryOnlineDB.batchInsert(platformID, hostID, today, maxOnline, aveNum, minOnline);
				}
			}
		}
		return true;
	}

}
