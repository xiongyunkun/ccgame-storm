package com.yuhe.statics.oss_statics;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import com.yuhe.statics.oss_statics.bolt.LogBolt;
import com.yuhe.statics.oss_statics.spout.RedisSpout;

public class StaticsTopology {
	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new RedisSpout(), 1);
		builder.setBolt("bolt",  new LogBolt(), 10).shuffleGrouping("spout");
		Config conf = new Config();
		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {
			conf.setDebug(true);
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("oss-statics", conf, builder.createTopology());
			Thread.sleep(10000);
			cluster.shutdown();
		}
	}

}
