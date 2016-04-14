package com.yuhe.statics.oss_statics.db.log;

import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

public abstract class AbstractDB {
	
	public abstract ResultSet query(Map<String, String> options);
	
	public abstract boolean batchInsert(String platformID, List<Map<String, String>> results);
}
