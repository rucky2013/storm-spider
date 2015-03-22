package com.datadio.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datadio.storm.lib.DioConfig;
import com.datadio.storm.storage.PageCassandra;
import com.datadio.storm.storage.PageRedis;

public class RequeueSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	
	private final String redisHost;
	private final String cassHost;
	private final int urlKeyExpireSec;
	
	SpoutOutputCollector _collector;
    PageRedis redis_conn = null;
    PageCassandra cass_conn = null;
    
    private Queue<Long> rssQueue;
    private Queue<Long> urlQueue;
    
    private long cacheTimer = -1L;
    private int expireIntervalSec = 300; 
    private long lastUrlQTime = 0L;
    private long lastRssQTime = 0L;
    
    public static final Logger LOG = LoggerFactory.getLogger(RequeueSpout.class);
    
    public RequeueSpout(DioConfig config) {
    	redisHost = config.getRedisHost();
    	cassHost = config.getCassandraHost();
    	urlKeyExpireSec = config.getUrlKeyExpireSec();
    }
    
    @SuppressWarnings("rawtypes")
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        redis_conn = new PageRedis(redisHost);
        cass_conn = new PageCassandra(cassHost);
        rssQueue = new ConcurrentLinkedQueue<Long>();
        urlQueue = new ConcurrentLinkedQueue<Long>();
        
        new FetcherThread(rssQueue, PageCassandra.RSSQUERY).start();
        new FetcherThread(urlQueue, PageCassandra.URLQUERY).start();
    }	

    public void nextTuple() {
    	
    	long cT = System.currentTimeMillis() / 1000000;
    	
    	checkTimerExpire(System.currentTimeMillis());
   
    	LOG.info("Requeue current time : " + cT);
    	LOG.info("Next url check time : " + lastUrlQTime);
    	LOG.info("Next rss check time : " + lastRssQTime);
    	
    	// check keys for url
    	lastUrlQTime = checkQueue(PageRedis.URL_LAST_REQUEUE, PageCassandra.URLQUERY, cT, lastUrlQTime);
    	
    	// check keys for rss
    	lastRssQTime = checkQueue(PageRedis.RSS_LAST_REQUEUE, PageCassandra.RSSQUERY, cT, lastRssQTime);
    	
    	_collector.emit(new Values(1));
    	
    	Utils.sleep(30000);
    	
    }        

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("requeueCounter"));
    }
    
    
    private class FetcherThread extends Thread {
    	Queue<Long> queue;
    	int typeCode;
    	
    	public FetcherThread(Queue<Long> queue, int typeCode) {
    		this.queue = queue;
    		this.typeCode = typeCode;
    		this.setDaemon(true); // don't hang JVM on exit
            this.setName("ReEnqueueThread"); // use an informative name
    	}
    	
    	public void run() {
    		while (true) {
    			Long keyID = this.queue.poll();
    			if(keyID != null) {
    				buildQueue(keyID, typeCode);
    			} else {
    				try { Thread.sleep(100); } catch (Exception ex) {}
    				continue;
    			}
    		}
    	}
    }
    
    private long checkQueue(String queueName, int queueCode, long cT, long lT) {
    	
    	if(lT <= 0L) {
    		String lTStr = redis_conn.getNextQueueTime(queueName);
    		if(lTStr != null) {
    			lT = Long.parseLong(lTStr);
    		}
    	}
    	
    	if(lT > 0 && cT > lT) {
    		sendToQueryQueue(lT, cT, queueCode);
    		redis_conn.setNextQueueTime(queueName, String.valueOf(cT));
    		lT = cT;
    	}
    	
		return lT;
    }
    
    private void sendToQueryQueue(long lT, long cT, int typeCode) {
    	Queue<Long> sendQueue;
    	
    	switch (typeCode) {
		case PageCassandra.URLQUERY:
			sendQueue = urlQueue;
			break;
		case PageCassandra.RSSQUERY:
			sendQueue = rssQueue;
			break;
		default:
			sendQueue = urlQueue;
			break;
		}
    	
    	if(cT > lT) {
			int diff = (int) (cT - lT);
			// exclude current time
			for (int i = 1; i <= diff; i++) {
				long timeKey = cT - i;
				LOG.info(">>>> Enqueue" + timeKey +  " to queue : " + typeCode );
				sendQueue.add(timeKey);
			}
		}
    }
    
    private boolean buildQueue(long timeKey, int typeCode) {
    	
		Map<String, String> keys = cass_conn.getNextQuery(typeCode, timeKey);
		
		if(keys != null && !keys.isEmpty()) {
			LOG.info(">>>> Going to Requeue " + keys.size() + " keys for " + timeKey + " for queue: " + typeCode);
			switch (typeCode) {
			case PageCassandra.URLQUERY:
				redis_conn.addToQueue(keys, urlKeyExpireSec);
				break;
			case PageCassandra.RSSQUERY:
				redis_conn.addToRSSQueue(keys);
				break;
			default:
				redis_conn.addToQueue(keys, urlKeyExpireSec);
				break;
			}
			
			keys = null; // try to release the memory
		}
    	
		return true;
    }
    
    private void checkTimerExpire(long curT) {
    	if(cacheTimer < 0) {
    		cacheTimer = curT;
    	} else {
    		// update timer from redis to expire cache
    		if(cacheTimer + expireIntervalSec*1000 < curT) {
    			LOG.info("Going to Refresh timer for requeue.");
    			String lTStrUrl = redis_conn.getNextQueueTime(PageRedis.URL_LAST_REQUEUE);
    			if(lTStrUrl != null) {
    				lastUrlQTime = Long.parseLong(lTStrUrl);
        		}
    			
    			String lTStrRSS = redis_conn.getNextQueueTime(PageRedis.RSS_LAST_REQUEUE);
    			if(lTStrRSS != null) {
    				lastRssQTime = Long.parseLong(lTStrRSS);
        		}
    			
    			cacheTimer += expireIntervalSec*1000;
    		}
    	}
    }
    
}