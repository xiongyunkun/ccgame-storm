package com.yuhe.mgame.db.statics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.yuhe.mgame.db.DBManager;


public class AddPlayerDB {
	/**
	 * 5分钟实时注册人数
	 * 
	 * @param platformID
	 * @param platformNum
	 * @param floorTime
	 * @return
	 */
	public boolean update5MinNum(String platformID, Map<String, String> map) {
		String[] cols = { "PlatformID", "HostID", "RegNum", "Time" };
		List<String> values = new ArrayList<String>();
		for (String col : cols) {
			values.add(map.getOrDefault(col, ""));
		}
		StringBuilder sb = new StringBuilder();
		sb.append("insert into ").append(platformID).append("_statics.tblAddPlayer(")
				.append(StringUtils.join(cols, ",")).append(") values('").append(StringUtils.join(values, "','"))
				.append("') on duplicate key update RegNum = RegNum + values(RegNum)");
		DBManager.execute(sb.toString());
		return true;
	}
}
