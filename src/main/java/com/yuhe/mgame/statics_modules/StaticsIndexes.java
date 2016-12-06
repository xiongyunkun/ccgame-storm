package com.yuhe.mgame.statics_modules;

import java.util.LinkedHashMap;
import java.util.Map;

public class StaticsIndexes {
	private static Map<String, AbstractStaticsModule> indexMap = new LinkedHashMap<String, AbstractStaticsModule>();

	static {
		// 这里添加各个统计指标
		indexMap.put("MixLoginStatics", new MixLoginStatics()); // 登陆登出相关指标统计，留存率、在线时长、等级
		indexMap.put("LoginStatics", new LoginStatics()); // 登陆过程分析
		indexMap.put("Retention", new Retention()); // 留存率
		indexMap.put("OnlineTime", new OnlineTime()); // 在线时长
//		indexMap.put("Gold", new GoldStatics()); // 钻石统计
		indexMap.put("HistoryOnline", new HistoryOnline());//历史在线统计
		indexMap.put("HistoryReg", new HistoryReg()); //历史注册统计
		indexMap.put("PayStatics", new PayStatics()); //充值统计入口
		indexMap.put("UserPayDay", new UserPayDay()); //玩家每日充值
		indexMap.put("PayDay", new PayDay());//单服单日充值统计
		indexMap.put("PayZone", new PayZone()); //充值区间统计
	};

	/**
	 * 返回统计指标的Map
	 * 
	 * @return
	 */
	public static Map<String, AbstractStaticsModule> GetIndexMap() {
		return indexMap;
	}
}
