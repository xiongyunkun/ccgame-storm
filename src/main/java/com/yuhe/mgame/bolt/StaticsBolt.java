package com.yuhe.mgame.bolt;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;

import com.yuhe.mgame.statics_modules.AbstractStaticsModule;
import com.yuhe.mgame.statics_modules.StaticsIndexes;



public class StaticsBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;
	// 多久触发一次tick tuple定时器操作
	private static final int EmitFrequencyInSeconds = 30;

	public void execute(Tuple input, BasicOutputCollector collector) {
		if (!TupleUtils.isTick(input)) {
			//处理log日志统计
			String staticsIndex = input.getString(0);
			@SuppressWarnings("unchecked")
			Map<String, List<Map<String, String>>> platformResults = (Map<String, List<Map<String, String>>>) input
					.getValue(1);
			Map<String, AbstractStaticsModule> indexMap = StaticsIndexes.GetIndexMap();
			AbstractStaticsModule module = indexMap.get(staticsIndex);
			if (module != null) {
				module.execute(platformResults);
			}
		} else {
			//定时执行，将统计数据存入数据库
			Map<String, AbstractStaticsModule> indexMap = StaticsIndexes.GetIndexMap();
			Iterator<String> it = indexMap.keySet().iterator();
			while(it.hasNext()){
				String moduleName = it.next();
				AbstractStaticsModule module = indexMap.get(moduleName);
				module.cronExecute();
			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("staticsIndex"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, EmitFrequencyInSeconds);
		return conf;
	}

}
