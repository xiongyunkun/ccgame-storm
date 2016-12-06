package com.yuhe.mgame.log_modules;

import java.util.LinkedHashMap;
import java.util.Map;

public class LogIndexes {
	private static Map<String, AbstractLogModule> indexMap = new LinkedHashMap<String, AbstractLogModule>();
	static {
		//这里添加各个统计指标
		indexMap.put("addgold", new GoldLog()); //钻石日志
		indexMap.put("subgold", new GoldLog()); //钻石日志
		indexMap.put("addmoney", new MoneyLog()); //金币日志
		indexMap.put("submoney", new MoneyLog()); //金币日志
		indexMap.put("addplayer", new AddPlayerLog()); //注册日志
		indexMap.put("online", new Online()); //5分钟在线
		indexMap.put("login", new LoginLog()); //登陆日志
		indexMap.put("logout", new LogoutLog()); //退出日志
		indexMap.put("userlevelup", new LevelUpLog()); // 升级日志
		indexMap.put("rename", new ReNameLog()); //改名日志
		indexMap.put("additem", new ItemLog()); //物品日志
		indexMap.put("subitem", new ItemLog()); //物品日志
		indexMap.put("mail", new MessageLog()); //邮件日志
		indexMap.put("stage", new InstanceLog()); //副本关卡
		indexMap.put("task", new TaskLog()); //任务日志
		indexMap.put("guide", new ClientLoadLog()); //客户端登陆日志
		indexMap.put("addpower", new PowerLog()); //体力日志
		indexMap.put("subpower", new PowerLog()); //体力日志
		indexMap.put("shopbuy", new ShopBuyLog()); //商店购买日志
		indexMap.put("arena", new ArenaLog()); //竞技场日志
		indexMap.put("luckydraw", new LuckyDrawLog()); //酒馆日志
		indexMap.put("etchallenge", new ChallengeLog()); //极限挑战
		indexMap.put("addcard", new CardLog()); //英雄日志
		indexMap.put("chat", new ChatLog()); //聊天日志
		indexMap.put("streetfighter", new StreetFighterLog()); //街霸日志
		indexMap.put("topchallenge", new TopChallengeLog()); //极致领域日志
		indexMap.put("worldslay", new WorldSlayLog()); //世界斩杀日志
		indexMap.put("www", new W3Log()); //3w日志
	}
	/**
	 * 返回统计指标的Map
	 * @return
	 */
	public static Map<String, AbstractLogModule> GetIndexMap(){
		return indexMap;
	}
}
