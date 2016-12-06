package com.yuhe.mgame;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.Broker;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StaticHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.trident.GlobalPartitionInformation;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.yuhe.mgame.bolt.KafkaLogBolt;
//import com.yuhe.mgame.bolt.RedisLogBolt;
//import com.yuhe.mgame.spout.RedisSpout;

import com.yuhe.mgame.bolt.StaticsBolt;

public class StaticsTopology {

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = createKafkaTopologyBuilder();
		Config conf = new Config();
		if (args != null && args.length > 0) {
			conf.setNumWorkers(1);
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {
			conf.setDebug(true);
			conf.setMaxTaskParallelism(1);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("oss-statics", conf, builder.createTopology());
			Thread.sleep(10000);
		}
	}

	/**
	 * 初始化Redis版的TopologyBuilder
	 * 
	 * @return
	 */
//	private static TopologyBuilder createRedisTopologyBuilder() {
//		TopologyBuilder builder = new TopologyBuilder();
//		builder.setSpout("spout", new RedisSpout(), 1);
//		builder.setBolt("log", new RedisLogBolt(), 10).shuffleGrouping("spout");
//		builder.setBolt("statics", new StaticsBolt(), 10).fieldsGrouping("log", new Fields("staticsIndex"));
//		return builder;
//	}

	/**
	 * 初始化kafka版的TopologyBuilder
	 * 
	 * @return
	 */
	private static TopologyBuilder createKafkaTopologyBuilder() {
		TopologyBuilder builder = new TopologyBuilder();
		SpoutConfig spoutConfig = initKafkaSpoutConfig();

		builder.setSpout("spout", new KafkaSpout(spoutConfig), 1);
		builder.setBolt("log", new KafkaLogBolt(), 10).shuffleGrouping("spout");
		builder.setBolt("statics", new StaticsBolt(), 10).fieldsGrouping("log", new Fields("staticsIndex"));
		return builder;
	}

	/**
	 * 初始化KafkaSpoutConfig
	 * 
	 * @return
	 */
	private static SpoutConfig initKafkaSpoutConfig() {
		String topic = "logstash";
		GlobalPartitionInformation info = new GlobalPartitionInformation(topic);
		info.addPartition(0, new Broker("192.168.1.97", 9092));
		BrokerHosts brokerHosts = new StaticHosts(info);
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, "/data/tmp/kafka", // 偏移量offset的根目录
				"kafka");// 子目录对应一个应用
		spoutConfig.ignoreZkOffsets = false;// 从头开始消费，实际上是要改成false的
		spoutConfig.socketTimeoutMs = 60;
		spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		return spoutConfig;
	}

}
