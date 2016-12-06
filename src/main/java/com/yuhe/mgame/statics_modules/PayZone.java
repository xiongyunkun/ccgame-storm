package com.yuhe.mgame.statics_modules;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;

import com.yuhe.mgame.db.DBManager;
import com.yuhe.mgame.db.ServerDB;
import com.yuhe.mgame.db.log.CommonDB;
import com.yuhe.mgame.db.statics.PayZoneDB;
import com.yuhe.mgame.utils.DateUtils2;

public class PayZone extends AbstractStaticsModule {
	private Map<String, Map<String, Integer>> HostZoneMap = new HashMap<String, Map<String, Integer>>();
	private Map<String, String> HostUpdateMap = new HashMap<String, String>();
	private static final Map<String, int[]> ZONE_MAP = new HashMap<String, int[]>() {
		private static final long serialVersionUID = 1L;

		{
			put("1", new int[] { 1, 9 });
			put("2", new int[] { 10, 49 });
			put("3", new int[] { 50, 99 });
			put("4", new int[] { 100, 199 });
			put("5", new int[] { 200, 499 });
			put("6", new int[] { 500, 999 });
			put("7", new int[] { 1000, 1999 });
			put("8", new int[] { 2000, 4999 });
			put("9", new int[] { 5000, 9999 });
			put("10", new int[] { 10000, 19999 });
			put("11", new int[] { 20000, 49999 });
			put("12", new int[] { 50000 });
		}
	};

	@Override
	public boolean execute(Map<String, List<Map<String, String>>> platformResults) {
		Iterator<String> pIt = platformResults.keySet().iterator();
		while (pIt.hasNext()) {
			String platformID = pIt.next();
			List<Map<String, String>> platformResult = platformResults.get(platformID);
			Map<String, Map<String, Integer>> hostPayMap = new HashMap<String, Map<String, Integer>>();
			for (Map<String, String> userPay : platformResult) {
				String uid = userPay.get("Uid");
				String hostID = userPay.get("HostID");
				int gold = Integer.parseInt(userPay.get("Gold"));
				Map<String, Integer> userPayMap = hostPayMap.get(hostID);
				if (userPayMap == null) {
					userPayMap = new HashMap<String, Integer>();
					hostPayMap.put(hostID, userPayMap);
				}
				gold += userPayMap.getOrDefault(uid, 0);
				userPayMap.put(uid, gold);
			}
			Iterator<String> hIt = hostPayMap.keySet().iterator();
			while (hIt.hasNext()) {
				String hostID = hIt.next();
				Map<String, Integer> zoneMap = HostZoneMap.get(hostID);
				if (zoneMap == null) {
					zoneMap = getZoneMapFromDB(platformID, hostID);
					HostZoneMap.put(hostID, zoneMap);
				}
				Map<String, Integer> userPayMap = hostPayMap.get(hostID);
				Iterator<String> uIt = userPayMap.keySet().iterator();
				while (uIt.hasNext()) {
					String uid = uIt.next();
					int gold = userPayMap.get(uid);
					int totalGold = getTotalGoldFormDB(platformID, hostID, uid);
					int orgGold = Math.max(totalGold - gold, 0);
					String orgZoneID = getZoneID(orgGold);
					String newZoneID = getZoneID(totalGold);
					if (orgZoneID == null || !newZoneID.equals(orgZoneID)) {
						if (orgZoneID != null) {
							int orgNum = zoneMap.getOrDefault(orgZoneID, 0);
							orgNum = Math.max(orgNum - 1, 0);
							zoneMap.put(orgZoneID, orgNum);
						}
						int newNum = zoneMap.getOrDefault(newZoneID, 0);
						zoneMap.put(newZoneID, newNum + 1);
					}
				}
				// 将区间统计结果写入数据库
				String time = DateUtils2.getTimeStr((int) (System.currentTimeMillis() / 1000));
				String[] times = StringUtils.split(time, " ");
				String today = times[0];
				PayZoneDB.insert(platformID, hostID, today, zoneMap);
				HostUpdateMap.put(hostID, today);
			}
		}

		return true;
	}

	/**
	 * 从tblUserPayStatics表中获得玩家的首抽时间和总充值金额
	 * 
	 * @param platformID
	 * @param hostID
	 * @param uid
	 * @return
	 */
	private int getTotalGoldFormDB(String platformID, String hostID, String uid) {
		int totalGold = 0;
		String tblName = platformID + "_statics.tblUserPayStatics";
		List<String> options = new ArrayList<String>();
		options.add("Uid = '" + uid + "'");
		Connection conn = DBManager.getConn();
		ResultSet resultSet = CommonDB.query(conn, tblName, options);
		try {
			while (resultSet.next()) {
				totalGold = resultSet.getInt("TotalGold");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			DBManager.closeConn(conn);
		}
		return totalGold;
	}

	/**
	 * 查询tblPayZone获得最近的充值区间统计数据
	 * 
	 * @param platformID
	 * @param hostID
	 * @return
	 */
	private Map<String, Integer> getZoneMapFromDB(String platformID, String hostID) {
		Map<String, Integer> zoneMap = new HashMap<String, Integer>();
		String nowTime = DateUtils2.getTimeStr((int) (System.currentTimeMillis() / 1000));
		String[] times = StringUtils.split(nowTime, " ");
		String today = times[0];
		// 先判断数据库里面有没有今天的充值区间统计
		String tblName = platformID + "_statics.tblPayZone";
		List<String> options = new ArrayList<String>();
		options.add("HostID = '" + hostID + "'");
		options.add("Date = '" + today + "'");
		Connection conn = DBManager.getConn();
		ResultSet resultSet = CommonDB.query(conn, tblName, options);
		try {
			while (resultSet.next()) {
				String zoneID = resultSet.getString("ZoneID");
				int userNum = resultSet.getInt("PayUserNum");
				zoneMap.put(zoneID, userNum);
			}
			if (zoneMap.size() == 0) {
				// 今天的数据为空，再获取昨天的数据
				String yesterday = DateUtils2.getOverDate(today, -1);
				options.remove(1);// 删除之前的查询今天的查询条件
				options.add("Date = '" + yesterday + "'");
				resultSet = CommonDB.query(conn, tblName, options);
				while (resultSet.next()) {
					String zoneID = resultSet.getString("ZoneID");
					int userNum = resultSet.getInt("PayUserNum");
					zoneMap.put(zoneID, userNum);
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			DBManager.closeConn(conn);
		}
		return zoneMap;
	}

	/**
	 * 获得充值钻石所在的充值区间ID
	 * 
	 * @param gold
	 * @return
	 */
	private String getZoneID(int gold) {
		String zoneID = null;
		Iterator<String> it = ZONE_MAP.keySet().iterator();
		while (it.hasNext()) {
			String tZoneID = it.next();
			int[] zones = ZONE_MAP.get(tZoneID);
			if (zones.length == 2 && gold >= zones[0] && gold < zones[1]) {
				zoneID = tZoneID;
				break;
			}
			if (zones.length == 1 && gold >= zones[0]) {
				zoneID = tZoneID;
				break;
			}

		}
		return zoneID;
	}

	@Override
	public boolean cronExecute() {
		synchronized (HostUpdateMap) {
			String today = DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd");
			Map<String, String> hostMap = ServerDB.getStaticsServers();
			Iterator<String> hIt = hostMap.keySet().iterator();
			while (hIt.hasNext()) {
				String hostID = hIt.next();
				String platformID = hostMap.get(hostID);
				String updateTime = HostUpdateMap.get(hostID);
				if (updateTime == null) {
					// 从数据库中获取数据
					Map<String, Integer> zoneMap = getZoneMapFromDB(platformID, hostID);
					HostZoneMap.put(hostID, zoneMap);
					HostUpdateMap.put(hostID, today);
					PayZoneDB.insert(platformID, hostID, today, zoneMap);
				} else if (!today.equals(updateTime)) {
					// 把昨天的数据拿过来就行
					Map<String, Integer> zoneMap = HostZoneMap.get(hostID);
					HostUpdateMap.put(hostID, today);
					PayZoneDB.insert(platformID, hostID, today, zoneMap);
				}
			}
		}
		return true;
	}

}
