package com.tikal.fullstack.thashtag.topology.bolts;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import backtype.storm.Constants;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class HashtagAggregatorBolt  extends BaseBasicBolt {
	private Map<Long, Map<String,Integer>> tagsCountings;
	
	private long intervalWindow;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("time-interval", "interval-tags-countings"));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		tagsCountings = new HashMap<>();
		intervalWindow = (long) stormConf.get("interval-window");
	}


	@Override
	public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
		if (isTickTuple(tuple)) {
			emitTags(outputCollector);
		} else {
			Long time = tuple.getLongByField("time");
			String hashtag = (String) tuple.getValueByField("hashtag");
			
			Long timeInterval = selectTimeInterval(time);			
			Map<String, Integer> intervalTagsCountings = getTagsCountingForInterval(timeInterval);
			Integer count = intervalTagsCountings.get(hashtag);
			count = (count==null)?1:count+1;
			intervalTagsCountings.put(hashtag, count);
		}
	}

	private boolean isTickTuple(Tuple tuple) {
		String sourceComponent = tuple.getSourceComponent();
		String sourceStreamId = tuple.getSourceStreamId();
		return sourceComponent.equals(Constants.SYSTEM_COMPONENT_ID)
				&& sourceStreamId.equals(Constants.SYSTEM_TICK_STREAM_ID);
	}

	private void emitTags(BasicOutputCollector outputCollector) {
		Long now = System.currentTimeMillis();
		Long emitUpToTimeInterval = selectTimeInterval(now);
		Set<Long> timeIntervalsAvailableToRemove = new HashSet<>();
		Set<Long> timeIntervalsAvailable = tagsCountings.keySet();
		
		for (Long timeInterval : timeIntervalsAvailable) {
			if (timeInterval <= emitUpToTimeInterval) {
				timeIntervalsAvailableToRemove.add(timeInterval);
				Map<String, Integer> intervalTagsCountings = tagsCountings.get(timeInterval);
				outputCollector.emit(new Values(timeInterval,intervalTagsCountings));
			}
		}
		for (Long key : timeIntervalsAvailableToRemove) 
			tagsCountings.remove(key);
	}

	private Long selectTimeInterval(Long time) {
		return time / (intervalWindow * 1000);
	}

	private Map<String,Integer> getTagsCountingForInterval(Long timeInterval) {
		Map<String,Integer> tagsCountingForInterval = tagsCountings.get(timeInterval);
		if (tagsCountingForInterval == null) {
			tagsCountingForInterval = new HashMap<>();
			tagsCountings.put(timeInterval, tagsCountingForInterval);
		}
		return tagsCountingForInterval;
	}
}