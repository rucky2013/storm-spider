package com.datadio.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.List;
import java.util.Map;

import com.datadio.storm.lib.DioConfig;
import com.datadio.storm.storage.PageRedis;

public class URLSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	
	private final int numUrlFetch;
	private final int numDomainFetch;
	private final String redisHost;
	SpoutOutputCollector _collector;
    PageRedis redis_conn = null;
    
    public URLSpout() {
    	redisHost = "localhost";
    	numDomainFetch = 1;
    	numUrlFetch = 100;
    }
    
    public URLSpout(DioConfig config) {
    	redisHost = config.getRedisHost();
    	numDomainFetch = (Integer)config.get("domainSpoutNum");
    	numUrlFetch = (Integer)config.get("urlSpoutNum");
    }
    
    @SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        redis_conn = new PageRedis(redisHost);
    }	

    public void nextTuple() {
    	
    	List<String> pageKeys = redis_conn.getUrlsFromDomains(numDomainFetch, numUrlFetch);
//    	System.out.println("Going to spout " + pageKeys.size() + " URLs");
    	if(pageKeys != null && !pageKeys.isEmpty()) {
    		_collector.emit(new Values(pageKeys));
    	} else {
    		Utils.sleep(300);
    	}
    }        

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("pageKeys"));
    }
    
}