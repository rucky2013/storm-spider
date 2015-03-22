package com.datadio.storm.scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.datadio.storm.lib.DioConfig;
import com.datadio.storm.lib.WebPage;
import com.datadio.storm.storage.PageCassandra;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class RSSGenerateBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	
	OutputCollector _collector;
	private final String cassHost;
	private FetchScheduler scheduler;
	PageCassandra cass_conn = null;
	
	public RSSGenerateBolt() {
    	cassHost = "127.0.0.1:9160";
    }
    
    public RSSGenerateBolt(DioConfig config) {
    	cassHost = config.getCassandraHost();
    }
    
    @SuppressWarnings("rawtypes")
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    	_collector = collector;
    	scheduler = new FetchScheduler();
    	cass_conn = new PageCassandra(cassHost);
    }

    @SuppressWarnings("unchecked")
    public void execute(Tuple tuple) {
    	List<String> pageKeys = (List<String>)tuple.getValue(0);
    	List<WebPage> feeds = cass_conn.getFeeds(pageKeys);
    	List<WebPage> scheduledFeeds = new ArrayList<WebPage>();
    	if(feeds != null && !feeds.isEmpty()) {
	    	for(WebPage feed : feeds) {
//	    		System.out.println("### Send page to partitioner: " + page.getUrl() + page.getFetchTime());
	    		if(scheduler.shouldFetch(feed, System.currentTimeMillis()) && feed.getDomainName() != null) {
	    			scheduledFeeds.add(feed);
	        	}
	    	}
	    	
	    	_collector.emit(tuple, new Values(scheduledFeeds));
    	}
        _collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("feeds"));
    }
}
