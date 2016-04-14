package com.yuhe.statics.oss_statics;

import com.yuhe.statics.oss_statics.bolt.LogBolt;
import com.yuhe.statics.oss_statics.spout.RedisSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class StaticsTopology {
	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new RedisSpout(), 1);
		builder.setBolt("bolt", new LogBolt(), 25).shuffleGrouping("spout");
		Config conf = new Config();
		conf.setDebug(true);
		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("oss-statics", conf, builder.createTopology());
			Thread.sleep(10000);
			cluster.shutdown();
		}
	}

}
