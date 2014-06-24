package com.tikal.fullstack.heatmap.topology.bolts;


import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
public class HashtagPersistorBolt extends BaseBasicBolt {
	private final Logger logger = LoggerFactory.getLogger(HashtagPersistorBolt.class);

	private Jedis jedis;
	private ObjectMapper objectMapper;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		jedis = new Jedis("localhost");
		objectMapper = new ObjectMapper();
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
		Long timeInterval = tuple.getLongByField("time-interval");
		Map<String, Integer> intervalTagsCountings = (Map<String, Integer>) tuple.getValueByField("interval-tags-countings");
		for (Map.Entry<String, Integer> entry : intervalTagsCountings.entrySet()) 
			jedis.hset(timeInterval.toString(), entry.getKey(), entry.getValue().toString());			
		jedis.publish("timeInterval", timeInterval.toString());
	}


	@Override
	public void cleanup() {
		jedis.quit();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	
}