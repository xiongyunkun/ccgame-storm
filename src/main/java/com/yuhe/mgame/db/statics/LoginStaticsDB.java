package com.yuhe.mgame.db.statics;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.yuhe.mgame.db.DBManager;

public class LoginStaticsDB {

	/**
	 * 记录登陆过程分析表
	 * 
	 * @param platformID
	 * @param hostID
	 * @param date
	 * @param hour
	 * @param stepNums
	 */
	public static void insert(String platformID, String hostID, String date, String hour,
			Map<String, Integer> stepNums) {
		List<String> values = new ArrayList<String>();
		Iterator<String> sIt = stepNums.keySet().iterator();
		while (sIt.hasNext()) {
			String step = sIt.next();
			int num = stepNums.get(step);
			String[] value = { platformID, hostID, date, hour, step, Integer.toString(num) };
			values.add(StringUtils.join(value, "','"));
		}
		String sql = "insert into " + platformID
				+ "_statics.tblLoginStatics(PlatformID, HostID, Date, Hour, Step, Num) values('"
				+ StringUtils.join(values, "'),('") + "') on duplicate key update Num = Num + values(Num)";
		DBManager.execute(sql);
	}

}
