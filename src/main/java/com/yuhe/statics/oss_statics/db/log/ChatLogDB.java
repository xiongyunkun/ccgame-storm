package com.yuhe.statics.oss_statics.db.log;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.yuhe.statics.oss_statics.db.DBManager;
import com.yuhe.statics.oss_statics.utils.DateUtils;

public class ChatLogDB extends AbstractDB {

	private static final String[] LOG_COLS = {"HostID", "Uid", "ChannelID", "Time", "Msg", "MD5"};

	@Override
	public ResultSet query(Map<String, String> options) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean batchInsert(String platformID, List<Map<String, String>> results) {
		Map<String, List<String>> dateMap = new HashMap<String, List<String>>();
		for (Map<String, String> result : results) {
			List<String> values = new ArrayList<String>();
			String dateStr = null;
			for (String col : LOG_COLS) {
				String value = result.get(col);
				if (col.equals("Time")) {
					dateStr = DateUtils.getSqlDate(value);
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
			sb.append("insert into ").append(platformID).append("_log.tblChatLog_").append(date).append("(")
					.append(StringUtils.join(LOG_COLS, ",")).append(") values('")
					.append(StringUtils.join(values, "'),('")).append("')");
			DBManager.execute(sb.toString());
		}
		return true;
	}

}
