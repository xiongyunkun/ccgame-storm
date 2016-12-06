package com.yuhe.mgame.db.statics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.yuhe.mgame.db.DBManager;

public class IMEIInfoDB {
	public static void insert(String platformID, List<Map<String, String>> imeiInfoList) {
		List<String> valueList = new ArrayList<String>();
		for(Map<String, String> imeiInfo:imeiInfoList){
			String[] strs = {imeiInfo.get("IMEI"), imeiInfo.get("Step"), imeiInfo.get("Time")};
			valueList.add(StringUtils.join(strs, "','"));
		}
		if (valueList.size() > 0) {
			StringBuilder sb = new StringBuilder();
			sb.append("insert into ").append(platformID).append("_statics.tblIMEIInfo(IMEI, Step, Time) values('")
					.append(StringUtils.join(valueList, "'),('")).append("') on duplicate key update Flag = 'true'");
			DBManager.execute(sb.toString());
		}
	}
}
