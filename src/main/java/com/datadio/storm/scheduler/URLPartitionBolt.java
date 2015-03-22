package com.datadio.storm.scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.datadio.storm.lib.DioConfig;
import com.datadio.storm.lib.WebPage;
import com.datadio.storm.storage.PageRedis;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class URLPartitionBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	
	OutputCollector _collector;
//	private String prevDomain;
//	private long prevFetchTime;
	private List<String>drainedDomains;
	private final int defaultDomainInterval;
	private final int initDomainInterval; // interval for the first time occured domain.
	private final String redisHost;
	private Map<String, Long> domainPrevFetchTime; // domain and its previous fetch time.
	
	private HashMap<String, LinkedList<WebPage>> waitQueueByDomain;
	
	PageRedis redis_conn = null;
	
	public URLPartitionBolt() {
		this(new DioConfig());  // 2s for each url in one domain, 5 min for init domain
	}
	
	public URLPartitionBolt(DioConfig config) {
		defaultDomainInterval = (Integer) config.get("defaultDomainInterval");
		initDomainInterval = (Integer) config.get("initDomainInterval");
		redisHost = config.getRedisHost();
	}
	
    @SuppressWarnings("rawtypes")
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    	_collector = collector;
    	drainedDomains = new ArrayList<String>();
    	waitQueueByDomain = new HashMap<String, LinkedList<WebPage>>();
    	redis_conn = new PageRedis(redisHost);
    	domainPrevFetchTime = new LinkedHashMap<String, Long>();
    }

    @SuppressWarnings({ "unchecked", "unused" })
	public void execute(Tuple tuple) {
 
//    	WebPage curPage = (WebPage)tuple.getValue(0);
    	
    	WebPage curPage = new WebPage((Map<String, Object>)tuple.getValue(0));
    	
    	long curTime = System.currentTimeMillis();
    	
    	if(curPage == null) {
//    		processWaitingQueue(curTime, tuple);
    	} else {
    	
	    	if(domainPrevFetchTime.containsKey(curPage.getDomainName())) {
//	    		System.out.println(">>>> Get url: " + curPage.getUrl() + " from: " + curPage.getDomainName());
	    		if(pastIntervalTime(curPage.getDomainName(), curTime)) {
	        		// parse the url here
	        		updateDomainFetchInfo(curPage.getDomainName(), curTime);
	        		_collector.emit(tuple, new Values(Collections.unmodifiableMap(curPage)));
	        	} else {
//	        		System.out.println(">>>>>>> Starting fetch url from waitQueue");
//	        		processWaitingQueue(curTime, tuple);
	        		addToWaitQueue(curPage);
	        	}
	    	} else { // first time
	    		long initCurTime = curTime - defaultDomainInterval*1000L + initDomainInterval*1000L;
	    		
//	    		System.out.println("!!!!!! Couldn't find domain for url: " + curPage.getUrl());
//	    		System.out.println("Going to set timer to: " + initCurTime);
	    		updateDomainFetchInfo(curPage.getDomainName(), initCurTime);
	    		_collector.emit(tuple, new Values(Collections.unmodifiableMap(curPage)));
	    	}
    	}
    	
    	processWaitingQueue(curTime, tuple);
        _collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("webPage"));
    }
    
    private void addToWaitQueue(WebPage page) {
    	LinkedList<WebPage>pageList = waitQueueByDomain.get(page.getDomainName());
    	if(pageList != null) {
    		pageList.add(page);
    	} else {
    		pageList = new LinkedList<WebPage>();
    		pageList.add(page);
    		waitQueueByDomain.put(page.getDomainName(), pageList);
    	}	
    }
    
    private void processWaitingQueue(long curTime, Tuple tuple) {
    	List<WebPage> waitPages = getNextURL(curTime);
		if(waitPages != null && !waitPages.isEmpty()) {
			// parse the url here
			for(WebPage waitPage : waitPages) {
//				System.out.println(">>>>>>> Found page from waitQueue: " + waitPage);
				updateDomainFetchInfo(waitPage.getDomainName(), curTime);
    			_collector.emit(tuple, new Values(Collections.unmodifiableMap(waitPage)));
			}
		}
		
		if(drainedDomains != null && !drainedDomains.isEmpty()){
//			System.out.println(">>>>>>> Add drained domains");
//			System.out.println(drainedDomain);
			redis_conn.addToDomainQueue(drainedDomains);
			drainedDomains.clear();
		}
    }
    
    // get the next available url, and also check to see if other on-scheduled domain is empty
    private List<WebPage> getNextURL(long curTime) {
    	List<WebPage> nextPages = new ArrayList<WebPage>();
    	Iterator<Map.Entry<String, Long>> it = domainPrevFetchTime.entrySet().iterator();
    	while(it.hasNext()) {
    		Map.Entry<String, Long> domain = it.next();
    		long prevFetchTime = domain.getValue();
			// reach scheduled time
			if( (curTime - prevFetchTime) >= (defaultDomainInterval*1000L)){
				String nextDomainName = domain.getKey();
				LinkedList<WebPage> domainPage = waitQueueByDomain.get(nextDomainName);
				
				if(domainPage!=null && domainPage.size() > 0) {
					nextPages.add(domainPage.pop());
				}else { // empty domain, re-enqueue to redis for re-fetching
					drainedDomains.add(nextDomainName);
					it.remove();
				}
			}
    	}
    	return nextPages;    	
    }
    
    private void updateDomainFetchInfo(String curDomain, long curTime) {
    	domainPrevFetchTime.put(curDomain, curTime);
    }
    
    private boolean pastIntervalTime(String domainName, long curTime) {
    	if(domainPrevFetchTime.containsKey(domainName)) {
    		long prevFetchTime = domainPrevFetchTime.get(domainName);
    		return (curTime - prevFetchTime) >= (defaultDomainInterval*1000L);
    	} else { // first time of this domain
//    		domainInterval.put(domainName, curTime);
    		return true;
    	}
    	
    }
}
