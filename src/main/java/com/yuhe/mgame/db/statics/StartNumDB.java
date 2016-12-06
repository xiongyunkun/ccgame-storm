package com.yuhe.mgame.db.statics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.shade.org.apache.commons.lang.StringUtils;

import com.yuhe.mgame.db.DBManager;

public class StartNumDB {
	private static final String W3_HOSTID = "666"; // 3w的日志记录的hostID

	public static void insert(String platformID, List<Map<String, String>> results) {
		List<String> valueList = new ArrayList<String>();
		String[] indexes = { "Model", "Brand", "DPI" };
		for (Map<String, String> result : results) {
			if (result.containsKey("Model")) {
				String date = result.get("Date");
				for (String index : indexes) {
					String[] value = { platformID, W3_HOSTID, date, index, result.get(index), "1" };
					valueList.add(StringUtils.join(value, "','"));
				}
			}
		}
		if (valueList.size() > 0) {
			StringBuilder sb = new StringBuilder();
			sb.append("insert into ").append(platformID)
					.append("_statics.tblStartNum(PlatformID, HostID, Date, `Index`, `CIndex`, Num) values('")
					.append(StringUtils.join(valueList, "'),('"))
					.append("') on duplicate key update Num = values(Num)");
			DBManager.execute(sb.toString());
		}
	}
}
