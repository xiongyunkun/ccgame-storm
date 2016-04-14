package com.yuhe.statics.oss_statics.spout;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.yuhe.statics.oss_statics.statics_modules.AbstractStaticsModule;
import com.yuhe.statics.oss_statics.statics_modules.StaticsIndexes;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import redis.clients.jedis.Jedis;

import org.apache.log4j.Logger;

public class RedisSpout extends BaseRichSpout {
	private SpoutOutputCollector collector;
	public static Logger logger = Logger.getLogger(RedisSpout.class);
	private static Jedis jedis = new Jedis("192.168.1.97", 6379);
	// lua脚本，用于在redis中批量获取队列内容
	private String LUA_SCRIPT = "local Result = {} local Length = redis.call('LLEN',KEYS[1]) "
			+ "for Index = 0, Length-1 do local Value = redis.call('LPOP',KEYS[1]) if Value then "
			+ "table.insert(Result, Value) end end return Result";

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}
	
	@SuppressWarnings("unchecked")
	public void nextTuple() {
		// get data from redis
		Map<String, AbstractStaticsModule> indexMap = StaticsIndexes.GetIndexMap();
		Iterator<String> it = indexMap.keySet().iterator();
		while (it.hasNext()) {
			String staticsIndex = (String) it.next();
			List<String> logList = (List<String>) jedis.eval(LUA_SCRIPT, 1, staticsIndex);
			collector.emit(new Values(staticsIndex, logList));
		}
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("staticsIndex", "logList"));
	}

}
