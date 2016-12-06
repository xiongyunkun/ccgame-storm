package com.yuhe.mgame.db.statics;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;

import com.yuhe.mgame.db.DBManager;

public class PayDB {
	// 5分钟实时在线表
	private static final String[] LOG5MIN_COLS = { "HostID", "PayCashNum", "PayUserNum", "Time", "Currency"};
	// 玩家充值统计表
	private static final String[] USER_PAY_COLS = { "Uid", "Urs", "Name", "HostID", "Currency", "TotalCashNum",
			"TotalNum", "TotalGold", "MinCashNum", "MaxCashNum", "FirstCashNum", "FirstCashTime", "LastCashNum",
			"LastCashTime" };

	/**
	 * 插入5分钟实时充值数据
	 * 
	 * @param platformID
	 * @param platformResult
	 * @return
	 */
	public boolean insert5Min(String platformID, Map<String, Map<String, String>> platformResult) {
		List<String> sqlValues = new ArrayList<String>();
		Iterator<String> hostIt = platformResult.keySet().iterator();
		while (hostIt.hasNext()) {
			String hostID = hostIt.next();
			Map<String, String> hostResult = platformResult.get(hostID);
			List<String> values = new ArrayList<String>();
			for (String col : LOG5MIN_COLS) {
				String value = hostResult.get(col);
				value = StringEscapeUtils.escapeSql(value);
				values.add(value);
			}
			sqlValues.add(StringUtils.join(values, "','"));
		}
		String sql = "insert into " + platformID + "_statics.tblPayActualTime(" + StringUtils.join(LOG5MIN_COLS, ",")
				+ ") values('" + StringUtils.join(sqlValues, "'),('")
				+ "') on duplicate key update PayCashNum = values(PayCashNum)+PayCashNum,PayUserNum = values(PayUserNum)+PayUserNum";
		DBManager.execute(sql);
		//监控代码，发现问题后及时删除
		String monitorSql = "select count(*) as Num from " + platformID + "_statics.tblPayActualTime where Currency = '' and Flag = 'true'";
		Connection conn = DBManager.getConn();
		ResultSet resultSet = DBManager.query(conn, monitorSql);
		try {
			if(resultSet.next() && resultSet.getInt("Num") != 0){
				String errSql = "insert into smcs.tblStaticsErr(ErrContent) values('" + sql +"')";
				DBManager.execute(errSql);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			DBManager.closeConn(conn);
		}
		//同时要去掉currency为空的数据
		String deleteSql = "update " + platformID + "_statics.tblPayActualTime set Flag = 'false' where Currency = ''";
		DBManager.execute(deleteSql);
		return true;
	}

	/**
	 * 插入玩家充值统计表
	 * 
	 * @param platformID
	 * @param platformResult
	 * @return
	 */
	public boolean insertUserPayStatics(String platformID, Map<String, Map<String, String>> platformResult) {
		List<String> sqlValues = new ArrayList<String>();
		Iterator<String> hostIt = platformResult.keySet().iterator();
		while (hostIt.hasNext()) {
			String hostID = hostIt.next();
			Map<String, String> hostResult = platformResult.get(hostID);
			List<String> values = new ArrayList<String>();
			for (String col : USER_PAY_COLS) {
				String value = hostResult.get(col);
				value = StringEscapeUtils.escapeSql(value);
				values.add(value);
			}
			sqlValues.add(StringUtils.join(values, "','"));
		}
		StringBuilder updateSql = new StringBuilder();
		updateSql.append("TotalCashNum = TotalCashNum + values(TotalCashNum),").append("TotalNum = TotalNum + values(TotalNum),")
		.append("TotalGold = TotalGold + values(TotalGold),").append("MinCashNum = least(MinCashNum, values(MinCashNum)),")
		.append("MaxCashNum = greatest(MaxCashNum, values(MaxCashNum)),").append("LastCashNum = values(LastCashNum),")
		.append("LastCashTime = values(LastCashTime),").append("Name = values(Name)");
		String sql = "insert into " + platformID + "_statics.tblUserPayStatics(" + StringUtils.join(USER_PAY_COLS, ",")
				+ ") values('" + StringUtils.join(sqlValues, "'),('")
				+ "') on duplicate key update " + updateSql.toString();
		DBManager.execute(sql);
		return true;
	}
}
