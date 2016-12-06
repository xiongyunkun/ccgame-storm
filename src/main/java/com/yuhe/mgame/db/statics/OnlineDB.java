package com.yuhe.mgame.db.statics;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;

import com.yuhe.mgame.db.DBManager;





public class OnlineDB{
	
	private static final String[] LOG_COLS = {"PlatformID", "HostID", "OnlineNum", "Time"};
	
	/**
	 * 查询在线人数,并且返回时间与在线人数的键值对
	 * @param platformID
	 * @param options
	 * @return
	 */
	public Map<String, Integer> queryNum(String platformID, Map<String, String> options){
		String where = " where Flag = 'true' ";
		if(options.containsKey("HostID"))
			where += " and HostID = '" + options.get("HostID") + "'";
		if(options.containsKey("StartTime"))
			where += " and Time >= '" + options.get("StartTime") + "'";
		if(options.containsKey("EndTime"))
			where += " and Time <= '" + options.get("EndTime") + "'";
		String sql = "select * from "+platformID+"_statics.tblOnline " + where;
		Connection conn = DBManager.getConn();
		ResultSet results = DBManager.query(conn, sql);
		Map<String, Integer> timeNumMap = new HashMap<String, Integer>();
		try {
			while (results.next()) {
				String timeStr = results.getString("Time");
				String[] times = timeStr.split("\\.");
				int onlineNum = results.getInt("OnlineNum");
				timeNumMap.put(times[0], onlineNum);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			DBManager.closeConn(conn);
		}
		return timeNumMap;
	}
	
	public boolean batchInsert(String platformID, List<Map<String, String>> results) {
		List<String> sqlValues = new ArrayList<String>();
		for(Map<String, String> result: results){
			List<String> values = new ArrayList<String>();
			for(String col : LOG_COLS){
				String value = result.get(col);
				value = StringEscapeUtils.escapeSql(value);
				values.add(value);
			}
			sqlValues.add(StringUtils.join(values, "','"));
			
		}
		String sql = "insert into "+platformID+"_statics.tblOnline("+StringUtils.join(LOG_COLS,  ",")
			+ ") values('" + StringUtils.join(sqlValues, "'),('") 
			+"') on duplicate key update OnlineNum = values(OnlineNum)";
		DBManager.execute(sql);
		
		return true;
	}
	 

}
