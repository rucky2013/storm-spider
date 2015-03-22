package com.datadio.storm.parser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.xerial.snappy.Snappy;

import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.datadio.storm.fetcher.RSSFetcher;
import com.datadio.storm.lib.DioCassandra;
import com.datadio.storm.lib.DioConfig;
import com.datadio.storm.lib.MD5Signature;
import com.datadio.storm.lib.WebPage;
import com.datadio.storm.scheduler.FetchScheduler;
import com.datadio.storm.storage.PageCassandra;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class RSSParseBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 1L;
	OutputCollector _collector;
	
	private final String cassHost;
	DioCassandra cass_conn = null;
	PageCassandra page_cass_conn = null;
	
	FetchScheduler scheduler;
	
	public RSSParseBolt(DioConfig config) {
    	cassHost = config.getCassandraHost();
    }
	
    @SuppressWarnings("rawtypes")
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    	_collector = collector;
    	cass_conn = new DioCassandra(cassHost);
    	page_cass_conn = new PageCassandra(cassHost);
    	scheduler = new FetchScheduler();
    }

    @SuppressWarnings("unchecked")
    public void execute(Tuple tuple) {
 
//    	WebPage feed = (WebPage)tuple.getValue(0);
    	WebPage feed = new WebPage((Map<String, Object>)tuple.getValue(0));
    	byte[] contentBytes = (byte[]) tuple.getValue(1);
    	
    	String linkMD5 = null;
    	if(contentBytes != null && contentBytes.length > 0) {
    		List<WebPage>pages = RSSFetcher.getFeedList(contentBytes);
    		List<String> pageKeys = new ArrayList<String>();
    		StringBuilder sb = new StringBuilder();
    		for(WebPage page : pages) {
    			String blogItemKey = page.getUniqKey();
    			pageKeys.add(blogItemKey);
    			sb.append(blogItemKey);
    		}
    		
    		linkMD5 = MD5Signature.getMD5(sb.toString());
    		
    		if(!pageKeys.isEmpty()) {
    			Map<String, Boolean>keyChecker = cass_conn.containsKeys(pageKeys, "Blog");
        		for(WebPage page : pages) {
//        			System.out.println(">>>>> Sending rss page:" + page.getUrl());
//        			System.out.println(">>>>> With title:" + page.getTitle());
        			if(!keyChecker.get(page.getUniqKey())) {
        				_collector.emit(new Values(Collections.unmodifiableMap(page)));
        			}
        		}
    		}
    	}
    	
    	// update feed scheduler
    	// new page or old page that is modified
    	if(linkMD5 == null || !linkMD5.equals(feed.getLinkMD5())){
    		feed.setStatus(FetchScheduler.STATUS_MODIFIED);
    		feed.setLinkMD5(linkMD5);
			if(feed.getModifiedTime() > 0) {
				feed.setPrevModifiedTime(feed.getModifiedTime());
			}
			feed.setModifiedTime(System.currentTimeMillis());
    	} else {
    		feed.setStatus(FetchScheduler.STATUS_NOTMODIFIED);
    	}
    	
    	scheduler.updateFetchScheduler(feed);
		page_cass_conn.updateFeed(feed);
		page_cass_conn.addToQuery(PageCassandra.RSSQUERY, System.currentTimeMillis()/1000, 
				feed.getUniqKey(), MD5Signature.getMD5(feed.getDomainName()));
		
        _collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("webpages"));
    }
	
}
