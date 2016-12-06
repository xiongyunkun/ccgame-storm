package com.yuhe.mgame.db.statics;

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.yuhe.mgame.db.DBManager;

public class GoldDB {
	public static boolean batchInsert(String platformID, String hostID, String date, String channel, String staticsType,
			Map<String, String> values) {
		StringBuilder sb = new StringBuilder();
		String value = values.getOrDefault("Value", "0");
		String uids = values.getOrDefault("Uids", "");
		String consumeNum = values.getOrDefault("ConsumeNum", "0");
		String[] sqlValues = { platformID, hostID, date, channel, staticsType, value, uids, consumeNum };
		sb.append("insert into ").append(platformID)
				.append("_statics.tblGold(PlatformID, HostID, Date, Channel, StaticsType, Value,Uids, ")
				.append("ConsumeNum) values('").append(StringUtils.join(sqlValues, "','"))
				.append("') on duplicate key update Value = '").append(value).append("', Uids = '").append(uids)
				.append("', ConsumeNum = '").append(consumeNum).append("'");
		DBManager.execute(sb.toString());
		return true;
	}

}
