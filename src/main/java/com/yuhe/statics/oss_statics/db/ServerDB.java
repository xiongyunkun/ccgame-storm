package com.yuhe.statics.oss_statics.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

public class ServerDB {
	//记录HostID与PlatformID的对应关系
	private static Map<String, String> HOST_MAP = new HashMap<String, String>();
	private static long HostLastUpdateTime = 0;
	private static int BENCH_DIFF = 300000; // 5分钟
	//记录SDKID与PlatformID的对应关系
	private static Map<String, String> SDK_MAP = new HashMap<String, String>();
	private static long SDKUpdateTime = 0;
	
	/**
	 * 获得统计服HostID的Map关系
	 * @return
	 */
	public static Map<String, String> getStaticsServers() {
		long nowTime = System.currentTimeMillis();
		if (nowTime - HostLastUpdateTime > BENCH_DIFF || HOST_MAP.size() == 0) {
			String sql = "select a.serverid as HostID, c.platformid as PlatformID from smcs.srvgroupinfo a, "
					+ "smcs.servergroup b, smcs.servers c where a.groupid = b.id and b.name = '统计专区' and a.serverid = c.hostid";
			Connection conn = DBManager.getConn();
			ResultSet results = DBManager.query(conn, sql);
			try {
				while (results.next()) {
					String hostID = results.getString("HostID");
					String platformID = results.getString("PlatformID");
					HOST_MAP.put(hostID, platformID);
				}
				HostLastUpdateTime = nowTime;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				DBManager.closeConn(conn);
			}
		}
		return HOST_MAP;
	}
	
	/**
	 * 根据SDKInfo字段获得platformID
	 * @param sdkInfo
	 * @return
	 */
	public static String getPlatformIDBySDKID(String sdkInfo){
		String platformID = "test";
		String[] sdkArray = StringUtils.split(sdkInfo, "_");
		if(sdkArray.length >= 1){
			String sID = sdkArray[0];
			long nowTime = System.currentTimeMillis();
			if(nowTime - SDKUpdateTime > BENCH_DIFF || SDK_MAP.size() == 0){
				//需要更新
				String sql = "select * from smcs.tblPlatform";
				Connection conn = DBManager.getConn();
				ResultSet results = DBManager.query(conn, sql);
				try {
					while (results.next()) {
						String sdkID = results.getString("SDKID");
						String pfID = results.getString("PlatformID");
						SDK_MAP.put(sdkID, pfID);
					}
					SDKUpdateTime = nowTime;
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					DBManager.closeConn(conn);
				}
			}
			if(SDK_MAP.containsKey(sID)){
				platformID = SDK_MAP.get(sID);
			}
		}
		return platformID;
	}
	
	private static Map<String,List<String>> HOST_PLATFORMS = new HashMap<String,List<String>>();
	private static long PlatformUpdateTime = 0;
	/**
	 * 获得该HostID对应的平台列表（因为有混服的情况）
	 * @param hostID
	 * @return
	 */
	public static List<String> getPlatformListByHostID(String hostID){
		long nowTime = System.currentTimeMillis();
		if(nowTime - PlatformUpdateTime > BENCH_DIFF || HOST_PLATFORMS.size() == 0){
			String sql = "select * from smcs.tblMixServers";
			Connection conn = DBManager.getConn();
			ResultSet results = DBManager.query(conn, sql);
			try {
				while (results.next()) {
					String tHostID = results.getString("HostID");
					String pfID = results.getString("PlatformID");
					List<String> pfIDList = HOST_PLATFORMS.getOrDefault(tHostID, new ArrayList<String>());
					pfIDList.add(pfID);
					HOST_PLATFORMS.put(tHostID, pfIDList);
				}
				PlatformUpdateTime = nowTime;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				DBManager.closeConn(conn);
			}
		}
		List<String> platformList = HOST_PLATFORMS.getOrDefault(hostID, new ArrayList<String>());
		return platformList;
	}
}
