package com.yuhe.mgame.bolt;

import java.util.List;
import java.util.Map;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.yuhe.mgame.db.ServerDB;
import com.yuhe.mgame.log_modules.AbstractLogModule;
import com.yuhe.mgame.log_modules.LogIndexes;

import net.sf.json.JSONObject;

public class KafkaLogBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;

	public void execute(Tuple input, BasicOutputCollector collector) {
		Map<String, List<Map<String, String>>> platformResults = null;
		String staticsIndex = null;
		String logStr = input.getString(0);
		JSONObject json = JSONObject.fromObject(logStr);
		String type = json.getString("type");
		Map<String, AbstractLogModule> indexMap = LogIndexes.GetIndexMap();
		if (indexMap.containsKey(type)) {
			AbstractLogModule module = indexMap.get(type);
			Map<String, String> hostMap = ServerDB.getStaticsServers();

			platformResults = module.execute4Kafka(json, hostMap);
			staticsIndex = module.getStaticsIndex();
		}

		if (staticsIndex != null && platformResults != null && platformResults.size() > 0) {
			collector.emit(new Values(staticsIndex, platformResults));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("staticsIndex", "logValues"));
	}

}
