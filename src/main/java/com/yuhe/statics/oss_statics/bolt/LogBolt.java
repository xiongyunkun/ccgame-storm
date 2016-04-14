package com.yuhe.statics.oss_statics.bolt;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.yuhe.statics.oss_statics.statics_modules.AbstractStaticsModule;
import com.yuhe.statics.oss_statics.statics_modules.StaticsIndexes;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;


public class LogBolt extends BaseBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static Logger logger = Logger.getLogger(LogBolt.class);

	/**
	 * 接收log日志数据，并且判断日志类型，采用对应的处理类型记录日志
	 */
	@SuppressWarnings("unchecked")
	public void execute(Tuple input, BasicOutputCollector collector) {
		String staticsIndex = input.getString(0);

		List<String> logList = (List<String>) input.getValue(1);
		if (logList.size() > 0) {
			// 根据taticsIndex用对应的处理模块处理
			Map<String, AbstractStaticsModule> indexMap = StaticsIndexes.GetIndexMap();
			AbstractStaticsModule module = indexMap.get(staticsIndex);
			if (module != null) {
				module.execute(logList);
			}
		}
//		collector.emit(new Values(staticsIndex));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("log"));

	}

}
