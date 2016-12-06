package com.yuhe.mgame.statics_modules;

import java.util.List;
import java.util.Map;

public class PayStatics extends AbstractStaticsModule {

	@Override
	public boolean execute(Map<String, List<Map<String, String>>> platformResults) {
		Map<String, AbstractStaticsModule> indexMap = StaticsIndexes.GetIndexMap();
		AbstractStaticsModule userPayDayModule = indexMap.get("UserPayDay"); //玩家单日充值统计
		userPayDayModule.execute(platformResults);
		AbstractStaticsModule payDayModule = indexMap.get("PayDay");//单服单日充值统计
		payDayModule.execute(platformResults);
		AbstractStaticsModule payZoneModule = indexMap.get("PayZone");//单服充值区间统计
		payZoneModule.execute(platformResults);
		return true;
	}

	@Override
	public boolean cronExecute() {
		// TODO Auto-generated method stub
		return false;
	}

}
