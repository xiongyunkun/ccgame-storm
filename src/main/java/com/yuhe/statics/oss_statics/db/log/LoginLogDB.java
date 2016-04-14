package com.yuhe.statics.oss_statics.db.log;

import java.sql.Connection;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.yuhe.statics.oss_statics.db.DBManager;
import com.yuhe.statics.oss_statics.utils.DateUtils;

public class LoginLogDB extends AbstractDB{
	
	private static final String[] LOG_COLS = {"HostID", "Uid", "Urs", "Ip", "Name", "Time"};
	
	@Override
	public ResultSet query(Map<String, String> options) {
		String platformID = options.get("PlatformID");
		StringBuilder where = new StringBuilder(" where Flag = 'true' ");
		Date dt=new Date();
		SimpleDateFormat matter1=new SimpleDateFormat("yyyy-MM-dd");
	    String startDate = matter1.format(dt);
	    String endDate = startDate; //初始状态下开始时间和结束时间相同
	    for(String key: options.keySet()){
	    	String value = options.get(key);
	    	if(!value.equals("")){
	    		if(key.equals("StartTime")){
	    			where.append(" and ").append(key).append(" >= '").append(value).append("'");
	    		}else if(key.equals("EndTime")){
	    			where.append(" and ").append(key).append(" <= '").append(value).append("'");
	    		}else if(Arrays.asList(LOG_COLS).contains(key)){
	    			where.append(" and ").append(key).append(" = '").append(value).append("'");
	    		}
	    	}
	    }
	    List<Date> dateList = DateUtils.getDates(startDate, endDate);
	    int length = dateList.size();
	    StringBuilder sql = new StringBuilder();
	    for(int i=0; i< length; i++){
	    	Date day = dateList.get(i);
	    	sql.append(" select * from ").append(platformID).append("._log.tblLoginLog_").append(day).append(where);
	    	if(i != length - 1){
	    		sql.append(" union "); //如果不是最后一个还要拼接union
	    	}
	    }
	    Connection conn = DBManager.getConn();
	    ResultSet results = DBManager.query(conn, sql.toString());
	    //DBManager.closeConn(conn);
	    return results;
	    
	}

	@Override
	public boolean batchInsert(String platformID, List<Map<String, String>> results) {
		Map<String, List<String>> dateMap = new HashMap<String, List<String>>();
		for(Map<String, String> result: results){
			List<String> values = new ArrayList<String>();
			String dateStr = null;
			for(String col : LOG_COLS){
				String value = result.get(col);
				if(col.equals("Time")){
					dateStr = DateUtils.getSqlDate(value);
				}
				values.add(value);
			}
			if(dateStr != null){
				List<String> sqls = dateMap.get(dateStr);
				if(sqls == null)
					sqls = new ArrayList<String>();
				sqls.add(StringUtils.join(values, "','"));
				dateMap.put(dateStr, sqls);
			}
		}
		//按日期入库
		Iterator<String> it = dateMap.keySet().iterator();
		while(it.hasNext()){
			String date = it.next();
			List<String> values = dateMap.get(date);
			StringBuilder sb = new StringBuilder();
			sb.append("insert into ").append(platformID).append("_log.tblLoginLog_")
			.append(date).append("(").append(StringUtils.join(LOG_COLS, ",")).append(") values('")
			.append(StringUtils.join(values, "'),('")).append("')");
			DBManager.execute(sb.toString()) ;
		}
		return true;
	}

}
