package com.yuhe.mgame.db.log;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;

import com.yuhe.mgame.db.DBManager;
import com.yuhe.mgame.utils.DateUtils2;


public class CommonDB {
	/**
	 * 按日期分表插入日志数据库
	 * 
	 * @param platformID
	 * @param results
	 * @param cols
	 * @param tblName
	 * @return
	 */
	public static boolean batchInsertByDate(String platformID, List<Map<String, String>> results, String[] cols,
			String tblName) {
		Map<String, List<String>> dateMap = new HashMap<String, List<String>>();
		for (Map<String, String> result : results) {
			List<String> values = new ArrayList<String>();
			String dateStr = null;
			for (String col : cols) {
				String value = result.get(col);
				value = StringEscapeUtils.escapeSql(value);
				if (col.equals("Time")) {
					dateStr = DateUtils2.getSqlDate(value);
				}
				values.add(value);
			}
			if (dateStr != null) {
				List<String> sqls = dateMap.get(dateStr);
				if (sqls == null)
					sqls = new ArrayList<String>();
				sqls.add(StringUtils.join(values, "','"));
				dateMap.put(dateStr, sqls);
			}
		}
		// 按日期入库
		Iterator<String> it = dateMap.keySet().iterator();
		while (it.hasNext()) {
			String date = it.next();
			List<String> values = dateMap.get(date);
			StringBuilder sb = new StringBuilder();
			sb.append("insert into ").append(platformID).append("_log.").append(tblName).append("_").append(date)
					.append("(").append(StringUtils.join(cols, ",")).append(") values('")
					.append(StringUtils.join(values, "'),('")).append("')");
			DBManager.execute(sb.toString());
		}
		return true;
	}
	/**
	 * 不分日期插入日志表
	 * 仅限没有按日期分表的模块调用，例如：banchats
	 * @param platformID
	 * @param results
	 * @param cols
	 * @param tblName
	 * @return
	 */
	public static boolean batchInsert(String platformID, List<Map<String, String>> results, String[] cols,
			String tblName) {
		List<String> sqlValues = new ArrayList<String>();
		for (Map<String, String> result : results) {
			List<String> values = new ArrayList<String>();
			for (String col : cols) {
				String value = result.get(col);
				value = StringEscapeUtils.escapeSql(value);
				values.add(value);
			}
			sqlValues.add(StringUtils.join(values, "','"));
		}
		StringBuilder sb = new StringBuilder();
		sb.append("insert into ").append(platformID).append("_log.").append(tblName).append("(")
				.append(StringUtils.join(cols, ",")).append(") values('").append(StringUtils.join(sqlValues, "'),('"))
				.append("')");
		DBManager.execute(sb.toString());
		return true;
	}
	/**
	 * 查询数据库返回结果
	 * @param tblName
	 * @param options
	 * @return
	 */
	public static ResultSet query(Connection conn, String tblName, List<String> options){
		String sql = "select * from " + tblName + " where 1 = 1 ";
		if(options.size() > 0){
			sql += " and " + StringUtils.join(options, " and ");
		}
		ResultSet results = DBManager.query(conn, sql);
		return results;
	}
}
