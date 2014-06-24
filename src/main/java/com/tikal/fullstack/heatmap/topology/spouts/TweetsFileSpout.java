package com.tikal.fullstack.heatmap.topology.spouts;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Stream;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TweetsFileSpout extends BaseRichSpout {
	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TweetsFileSpout.class);
	private Stream<String> lines;
	private SpoutOutputCollector outputCollector;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

	@Override
	public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
		try {
			this.outputCollector = spoutOutputCollector;
			lines = Files.lines(Paths.get("/Users/yanaif/dev/fuse2014/hashtag_processor/src/main/resources/tweets.json"));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void nextTuple() {		
//		lines.forEach(line -> System.out.println(line));
		lines.forEach(line -> outputCollector.emit(new Values(line)));
	}
	
	private void processLine(String line) throws InterruptedException{
		logger.debug("Got tweet :{}",line);
		outputCollector.emit(new Values(line));
		Thread.sleep(10000);		
	}
}
