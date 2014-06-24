package com.tikal.fullstack.thashtag.topology.bolts;

import java.util.HashSet;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONArray;
import org.json.JSONObject;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class HashtagTokenizerBolt  extends BaseBasicBolt {
	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashtagTokenizerBolt.class);

	@Override
	public void declareOutputFields(OutputFieldsDeclarer fieldsDeclarer) {
		fieldsDeclarer.declare(new Fields("time","hashtag"));
	}

	

	@Override
	public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
		logger.debug("Got:"+tuple);
		String tweet = tuple.getStringByField("tweet");
		long time = extractTime(tweet);
		Set<String> hashtags = extractTags(tweet);
		for (String tag : hashtags) 
			outputCollector.emit(new Values(time,tag));
	}

	
	public Set<String> extractTags(String line) {
        Set<String> out = new HashSet<String>();
        JSONObject obj = new JSONObject( line );
        if(!obj.has("hashtags"))
        	return out;
        JSONArray arr = obj.getJSONArray("hashtags");
        for ( int i = 0; i < arr.length(); i++ ) {
            out.add( arr.getJSONObject(i).getString("text") );
        }
        return out;
    }

    public  long extractTime(String line) {
        JSONObject obj = new JSONObject( line );
        String txt = obj.getString("created_at");
        //"Mon Jun 23 20:21:57 +0000 2014"
        DateTimeFormatter formatter = DateTimeFormat.forPattern("E MMM d HH:mm:ss Z yyyy");
        DateTime dt = formatter.parseDateTime(txt);
        return dt.getMillis();
    }

}
