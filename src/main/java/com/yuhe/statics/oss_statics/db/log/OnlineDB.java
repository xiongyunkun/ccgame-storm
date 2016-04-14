package com.yuhe.statics.oss_statics.db.log;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.yuhe.statics.oss_statics.db.DBManager;



public class OnlineDB extends AbstractDB{
	
	private static final String[] LOG_COLS = {"HostID", "OnlineNum", "Time", "PlatformID"};
	
	@Override
	public ResultSet query(Map<String, String> options) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean batchInsert(String platformID, List<Map<String, String>> results) {
		List<String> sqlValues = new ArrayList<String>();
		for(Map<String, String> result: results){
			List<String> values = new ArrayList<String>();
			for(String col : LOG_COLS){
				String value = result.get(col);
				values.add(value);
			}
			sqlValues.add(StringUtils.join(values, "','"));
			
		}
		String sql = "insert into "+platformID+"_statics.tblOnline("+StringUtils.join(LOG_COLS,  ",")
			+ ") values('" + StringUtils.join(sqlValues, "'),('") 
			+"') on duplicate key update OnlineNum = values(OnlineNum)";
		DBManager.execute(sql);
		
		return true;
	}
	 

}
