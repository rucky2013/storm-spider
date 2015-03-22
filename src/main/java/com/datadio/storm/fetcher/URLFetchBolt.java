package com.datadio.storm.fetcher;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import com.datadio.storm.lib.WebPage;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class URLFetchBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 1L;
	
	OutputCollector _collector;
	URLDiscover fetcher;

    @SuppressWarnings("rawtypes")
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    	_collector = collector;
		fetcher = new URLDiscover();
    }

    @SuppressWarnings("unchecked")
    public void execute(Tuple tuple) {
 
//    	WebPage page = (WebPage)tuple.getValue(0);
    	WebPage page = new WebPage((Map<String, Object>)tuple.getValue(0));
    	
		String inputUrl = page.getCleanedUrl();
		
		byte[] contentBytes = null;
		
		contentBytes = fetcher.fetchDocumment(inputUrl);
    		
    	if(contentBytes != null)
    		_collector.emit(tuple, new Values(Collections.unmodifiableMap(page), contentBytes));
 
        _collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("origin", "webpages"));
    }
	
}
