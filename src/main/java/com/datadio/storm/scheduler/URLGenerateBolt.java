package com.datadio.storm.scheduler;

import java.util.Collections;
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

public class URLGenerateBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	
	OutputCollector _collector;
	private final String cassHost;
	private FetchScheduler scheduler;
	PageCassandra cass_conn = null;
	
	public URLGenerateBolt() {
    	cassHost = "127.0.0.1:9160";
    }
    
    public URLGenerateBolt(DioConfig config) {
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
    	List<WebPage> webPages = cass_conn.getPages(pageKeys);
    	
    	if(webPages == null) {
//    		_collector.emit(tuple, new Values(new Object[]{ null, null }));
    	} else {
	    	for(WebPage page : webPages) {
//	    		System.out.println("### Send page to partitioner: " + page.getUrl() + page.getFetchTime());
	    		
	    		if( 
	    			page.getUrl() != null &&
	    			page.getCleanedUrl() != null && 
	    			page.getDomainName() != null &&
	    			scheduler.shouldFetch(page, System.currentTimeMillis())) {
	        		_collector.emit(tuple, new Values(Collections.unmodifiableMap(page), page.getDomainName()));
	        	}
	    	}
    	}
        _collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("webPage", "domain"));
    }
}
