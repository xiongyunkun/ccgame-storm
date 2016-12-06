package com.yuhe.mgame.db.statics;

import java.util.Map;

import com.yuhe.mgame.db.DBManager;
/**
 * 历史注册表tblHistoryReg相关逻辑
 * @author xiongyunkun
 *
 */
public class HistoryRegDB {
	/**
	 * 插入历史注册表tblHistoryReg
	 * @param platformID
	 * @param hostID
	 * @param date
	 * @param numMap
	 * @return
	 */
	public static boolean batchInsert(String platformID, String hostID, String date, Map<String, Integer> numMap) {
		int regNum = numMap.get("RegNum");
		int totalRegNum = numMap.get("TotalRegNum");
		StringBuilder sb = new StringBuilder();
		sb.append("insert into ").append(platformID)
				.append("_statics.tblHistoryReg(PlatformID, HostID, Date, RegNum, TotalRegNum)")
				.append(" values('").append(platformID).append("','").append(hostID).append("','").append(date)
				.append("','").append(regNum).append("','")
				.append(totalRegNum).append("') on duplicate key update RegNum = '").append(regNum)
				.append("', TotalRegNum = '")
				.append(totalRegNum).append("'");
		DBManager.execute(sb.toString());
		return true;
	}
}
