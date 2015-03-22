package com.datadio.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.datadio.storm.lib.DioConfig;
import com.datadio.storm.storage.PageRedis;

public class RSSSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	
	private final int numUrlFetch;
	SpoutOutputCollector _collector;
	private final String redisHost;
	PageRedis redis_conn = null;
//    private BlockingDeque<String> toSend;
    public static final Logger LOG = LoggerFactory.getLogger(RSSSpout.class);
    
    public RSSSpout() {
    	numUrlFetch = 1;
    	redisHost = "localhost";
    }
    
    public RSSSpout(DioConfig config) {
    	numUrlFetch = (Integer)config.get("urlSpoutNum");
//    	numUrlFetch = 1;
    	redisHost = config.getRedisHost();
    
    }
    
    @SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        redis_conn = new PageRedis(redisHost);
//        toSend = new LinkedBlockingDeque<String>();
        
//        new Thread(new Runnable() {
//        	public void run() {
////        		JedisPoolConfig redis_conf = new JedisPoolConfig();
////        		redis_conf.setMaxActive(5);
////            	JedisPool redis_pool = new JedisPool(redis_conf, redisHost);
//        		Jedis client = new Jedis(redisHost);
//        		while(true){
////        			Jedis jedis_conn = redis_pool.getResource();
//        			try{
//        				List<String> pageKeys = client.lrange(PageRedis.RSS_QUEUE, 0, numUrlFetch-1);
//    					if(pageKeys != null && !pageKeys.isEmpty()) {
//        					client.ltrim(PageRedis.RSS_QUEUE, numUrlFetch, -1);
//        				}
//        				
////        				List<String> pageKeys = client.blpop(1, PageRedis.RSS_QUEUE);
//        				
//        				if(pageKeys != null && !pageKeys.isEmpty()) {
//        					toSend.addAll(pageKeys);
////        					toSend.add(pageKeys.get(1));
//        				}
//        			}catch(Exception e){
//        				LOG.error("Error reading queues from redis",e);
//        				try {
//        					Thread.sleep(100);
//        				} catch (InterruptedException e1) {}
//        			}
//        		}
//        	}
//        }).start();
    }	

    public void nextTuple() {
    	
    	List<String> pageKeys = redis_conn.getFeeds(numUrlFetch);
    	if(pageKeys != null && !pageKeys.isEmpty()) {
        	System.out.println(" -> -> -> Going to spout " + pageKeys.size() + " URLs");
    		_collector.emit(new Values(pageKeys));
    	} else {
//    		_collector.emit(new Values(new Object[]{ null }));
//    		try { Thread.sleep(300); } catch (InterruptedException e) {}
//    		return;
    		Utils.sleep(300);
    	}
    	
//    	if(!toSend.isEmpty()) {
//    		String pageKey = toSend.removeFirst();
//    		LOG.debug("Going to spout" + pageKey);
//			_collector.emit(new Values(pageKey));
//    	}
    }        

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("feeds"));
    }
    
}