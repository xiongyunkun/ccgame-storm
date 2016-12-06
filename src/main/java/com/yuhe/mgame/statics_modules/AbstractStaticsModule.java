package com.yuhe.mgame.statics_modules;

import java.util.List;
import java.util.Map;

public abstract class AbstractStaticsModule {
	/**
	 * log日志统计
	 * @param platformResults
	 * @return
	 */
	public abstract boolean execute(Map<String, List<Map<String, String>>> platformResults);
	/**
	 * 定时执行逻辑
	 * @return
	 */
	public abstract boolean cronExecute();

}
