package com.yuhe.mgame.db.statics;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.yuhe.mgame.db.DBManager;

public class PayZoneDB {
	public static boolean insert(String platformID, String hostID, String date, Map<String, Integer> zoneMap) {
		List<String> sqlValues = new ArrayList<String>();
		Iterator<String> it = zoneMap.keySet().iterator();
		while (it.hasNext()) {
			String zoneID = it.next();
			int userNum = zoneMap.get(zoneID);
			String[] values = { platformID, hostID, date, zoneID, Integer.toString(userNum) };
			sqlValues.add(StringUtils.join(values, "','"));
		}
		if(sqlValues.size() > 0){
			String sql = "insert into " + platformID
					+ "_statics.tblPayZone(PlatformID, HostID, Date, ZoneID, PayUserNum) values('"
					+ StringUtils.join(sqlValues, "'),('") + "') on duplicate key update PayUserNum = values(PayUserNum)";
			DBManager.execute(sql);
		}
		
		return true;
	}
}
