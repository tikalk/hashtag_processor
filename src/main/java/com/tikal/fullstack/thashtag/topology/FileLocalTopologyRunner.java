package com.tikal.fullstack.thashtag.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.tikal.fullstack.thashtag.topology.bolts.HashtagAggregatorBolt;
import com.tikal.fullstack.thashtag.topology.bolts.HashtagPersistorBolt;
import com.tikal.fullstack.thashtag.topology.bolts.HashtagTokenizerBolt;
import com.tikal.fullstack.thashtag.topology.spouts.TweetsFileSpout;

public class FileLocalTopologyRunner {
	public static void main(String[] args) {
		int intervalWindow = 2;
		int emitInterval = 3;

		TopologyBuilder builder = buildTopolgy(intervalWindow, emitInterval);


		Config config = new Config();
		config.setDebug(false);

		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("thashtags", config, builder.createTopology());
	}

	private static TopologyBuilder buildTopolgy(int intervalWindow, int emitInterval) {
		
		
		Config heatmapConfig = new Config();
		heatmapConfig.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitInterval);
		heatmapConfig.put("interval-window", intervalWindow);
		
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("tweets", new TweetsFileSpout());
		builder.setBolt("tokenizer", new HashtagTokenizerBolt()).shuffleGrouping("tweets");
		builder.setBolt("aggregator", new HashtagAggregatorBolt()).fieldsGrouping("tokenizer",new Fields("hashtag")).addConfigurations(heatmapConfig );
		builder.setBolt("persistor", new HashtagPersistorBolt()).shuffleGrouping("aggregator");
		
		return builder;
	}
}