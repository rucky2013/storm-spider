package com.datadio.storm.storage;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datadio.storm.lib.DioConfig;
import com.datadio.storm.lib.WebPage;
import com.datadio.storm.scheduler.FetchScheduler;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
//import backtype.storm.tuple.Values;

public class URLFinalizeBolt extends BaseRichBolt{
	private static final long serialVersionUID = 1L;
	
	OutputCollector _collector;
	private final String cassHost;
	private final String redisHost;
	private final int urlKeyExpireSec;
	private final int defaultPageInterval;
	PageCassandra cass_conn = null;
	PageRedis redis_conn = null;
	FetchScheduler scheduler;
	public static final Logger LOG = LoggerFactory.getLogger(URLFinalizeBolt.class);

    public URLFinalizeBolt(DioConfig config) {
    	redisHost = config.getRedisHost();
    	cassHost = config.getCassandraHost();
    	urlKeyExpireSec = config.getUrlKeyExpireSec();
    	defaultPageInterval = (Integer) config.get("defaultPageInterval");
    }
	
    @SuppressWarnings("rawtypes")
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    	_collector = collector;
        cass_conn = new PageCassandra(cassHost);
        redis_conn = new PageRedis(redisHost);
        scheduler = new FetchScheduler();
    }

    @SuppressWarnings("unchecked")
    public void execute(Tuple tuple) {
//    	WebPage originPage = (WebPage)tuple.getValue(0);
    	WebPage originPage = new WebPage((Map<String, Object>)tuple.getValue(0));
		Map<String,Object> foundLinks = (Map<String,Object>)tuple.getValue(1);
		List<String> links = (List<String>) foundLinks.get("url");
    	List<String> rss = (List<String>) foundLinks.get("rss");
    	List<String> social = (List<String>) foundLinks.get("social");
		
    	
    	if(links != null && !links.isEmpty()) {
    		Set<String> externalDomainSet = new HashSet<String>();

    		for(String link : links) {
        		WebPage newPage = new WebPage(link, 0, defaultPageInterval);
        		
        		if(newPage.getDomainName() != null) {
	        		// check if in the same domain
	        		if( newPage.getDomainName().equals(originPage.getDomainName())) {
	        			
	            		if(!cass_conn.containsPage(newPage)) {
	            			
	            			if(originPage.getFetchTime() > 1) {
	            				newPage.setFetchTime(1L); // new page in the same domain that is going to score.
	            			}
	            			
	                		LOG.debug(">>> going to save internal link: " + link);
	//                		System.out.println(link);
	                		// try to get rid of the problem when the url is like: "http:/"
	                		cass_conn.addNewPage(newPage);
	            			redis_conn.addToQueue(newPage, urlKeyExpireSec);
	            		 }
	        			
	        		} else {
	        			externalDomainSet.add(newPage.getDomainName());
	        		}
        		}
        	}
    		
    		if(!externalDomainSet.isEmpty()) {
    			for(String d: externalDomainSet) {
    				WebPage newDomainPage = new WebPage("http://" + d, 0, defaultPageInterval);
    				if(!cass_conn.containsPage(newDomainPage)) {
    					cass_conn.addNewPage(newDomainPage);
    				}
    				
    				LOG.debug(">>> going to enqueue external domain: " + newDomainPage.getUrl());
    				redis_conn.addToQueue(newDomainPage, urlKeyExpireSec);
    			}
    		}
    	}
    	
    	if(rss != null && !rss.isEmpty()) {
	    	for(String feedURL : rss) {
	    		WebPage newPage = new WebPage(feedURL);
//	    		LOG.info(">>> found rss link: " + feedURL);
//	    		LOG.info("<<< the origin link if:" + originPage.getUrl());
	    		
        		if(!cass_conn.containsFeed(newPage)) {
        			LOG.info(">>> found new rss link: " + feedURL + ":" + newPage.getUniqKey());
        			cass_conn.addFeed(newPage);
    	    		redis_conn.addToRSSQueue(newPage, urlKeyExpireSec);
        		}
	    	}
    	}
    	
    	if(social != null && !social.isEmpty())
    		cass_conn.addSocial(originPage.getDomainName(), social);
    	
        _collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("webpage"));
    }
}
