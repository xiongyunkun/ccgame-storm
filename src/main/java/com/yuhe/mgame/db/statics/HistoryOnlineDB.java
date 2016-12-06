package com.yuhe.mgame.db.statics;

import com.yuhe.mgame.db.DBManager;
/**
 * 历史在线数据表操作
 * @author xiongyunkun
 *
 */
public class HistoryOnlineDB {
	/**
	 * 插入历史在线数据表
	 * @param platformID
	 * @param hostID
	 * @param date
	 * @param maxNum
	 * @param aveNum
	 * @param minNum
	 * @return
	 */
	public static boolean batchInsert(String platformID, String hostID, String date, int maxNum, int aveNum,
			int minNum) {
		StringBuilder sb = new StringBuilder();
		sb.append("insert into ").append(platformID)
				.append("_statics.tblHistoryOnline(PlatformID, HostID, Date, MaxOnline, AveOnline, MinOnline)")
				.append(" values('").append(platformID).append("','").append(hostID).append("','").append(date)
				.append("','").append(maxNum).append("','").append(aveNum).append("','").append(minNum)
				.append("') on duplicate key update MaxOnline = '").append(maxNum).append("', AveOnline = '")
				.append(aveNum).append("', MinOnline = '").append(minNum).append("'");
		DBManager.execute(sb.toString());
		return true;
	}

}
