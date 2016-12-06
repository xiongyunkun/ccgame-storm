package com.yuhe.mgame.bolt;

import java.util.List;
import java.util.Map;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.yuhe.mgame.log_modules.AbstractLogModule;
import com.yuhe.mgame.log_modules.LogIndexes;
import com.yuhe.mgame.db.ServerDB;





public class RedisLogBolt extends BaseBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * 接收log日志数据，并且判断日志类型，采用对应的处理类型记录日志
	 */
	@SuppressWarnings("unchecked")
	public void execute(Tuple input, BasicOutputCollector collector) {
		Map<String, List<Map<String, String>>> platformResults = null;
		String staticsIndex = null;
		String logIndex = input.getString(0);
		List<String> logList = (List<String>) input.getValue(1);
		if (logList.size() > 0) {
			// 根据taticsIndex用对应的处理模块处理
			Map<String, AbstractLogModule> indexMap = LogIndexes.GetIndexMap();
			AbstractLogModule module = indexMap.get(logIndex);
			if (module != null) {
				Map<String, String> hostMap = ServerDB.getStaticsServers();
				platformResults = module.execute(logList, hostMap);
				staticsIndex = module.getStaticsIndex();
			}
		}
		if (staticsIndex != null && platformResults != null && platformResults.size() > 0) {
			collector.emit(new Values(staticsIndex, platformResults));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("staticsIndex", "logValues"));
	}

}
