package com.datadio.storm.parser;

import java.util.Collections;
import java.util.Map;

//import org.xerial.snappy.Snappy;





import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datadio.storm.fetcher.URLDiscover;
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

public class URLParseBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 1L;
	private final String cassHost;
	OutputCollector _collector;
	URLDiscover fetcher;
	FetchScheduler scheduler;
	PageCassandra cass_conn = null;
	private static final Logger LOG = LoggerFactory.getLogger(URLParseBolt.class);
	
	public URLParseBolt(DioConfig config) {
    	cassHost = config.getCassandraHost();
    }
	
    @SuppressWarnings("rawtypes")
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    	_collector = collector;
		fetcher = new URLDiscover();
		scheduler = new FetchScheduler();
		cass_conn = new PageCassandra(cassHost);
    }

    @SuppressWarnings("unchecked")
	public void execute(Tuple tuple) {
 
//    	WebPage page = (WebPage)tuple.getValue(0);
    	
    	WebPage page = new WebPage((Map<String, Object>)tuple.getValue(0));
    	
//    	byte[] compressedBytes = (byte[]) tuple.getValue(1);
//    	byte[] contentBytes;
//		try {
//			contentBytes = Snappy.uncompress(compressedBytes);
//			Map<String,Object> foundLinks = fetcher.findLinks(page.getCleanedUrl(),contentBytes);
//	    	
//	    	if(foundLinks != null) {
////	    		page.setRawHtml((String)foundLinks.get("rawHtml"));
//	    		page.setRawHtmlCompressed(compressedBytes);
//	    		_collector.emit(tuple, new Values(page, foundLinks));
//	    	}
//	    	
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
    	
    	byte[] contentBytes = (byte[]) tuple.getValue(1);
//    	int pageStatus = tuple.getIntegerByField("status");
    	
    	if(contentBytes != null && contentBytes.length > 0) {
    		String docString = new String(contentBytes);
    		
    		String linkMD5 = URLDiscover.getLinkMD5(docString);
    		
    		// new page or old page that is modified
    		if(	page.getLinkMD5() == null ||
    			!page.getLinkMD5().equals(linkMD5)) {
    			
    			page.setStatus(FetchScheduler.STATUS_MODIFIED);
    			page.setLinkMD5(linkMD5);
    			
    			if(page.getModifiedTime() > 0) {
    				page.setPrevModifiedTime(page.getModifiedTime());
    			}
    			page.setModifiedTime(System.currentTimeMillis());
    			
    			Map<String,Object> foundLinks = fetcher.findLinks(page.getCleanedUrl(),docString);
//    			page.setRawHtml(docString);
    			if(foundLinks != null && !foundLinks.isEmpty())
    				_collector.emit(tuple, new Values(Collections.unmodifiableMap(page), Collections.unmodifiableMap(foundLinks)));
    			
    		} else {
    			// update fetch time info for this page
    			page.setStatus(FetchScheduler.STATUS_NOTMODIFIED);
    		}
    		
    		WebPage tmp_page = new WebPage(page);
    		
    		if(tmp_page.getFetchTime() > 0 ) {
        		int contentLength = URLDiscover.getContentLength(docString);
    	    	// new page or content of the page has been changed
    	    	if(	tmp_page.getContentLength() == -1 || 
    	    		tmp_page.getContentLength() != contentLength) {
    	  
    	    		tmp_page.setContentLength(contentLength);
    	    	}
    		}
    		
//    		LOG.debug("== Finished parsing page : " + tmp_page.getUrl());
//    		LOG.debug("== The fetch time changed from : " + tmp_page.getFetchTime());
    		scheduler.updateFetchScheduler(tmp_page);
//    		LOG.debug("== The fetch time changed to : " + tmp_page.getFetchTime());
	    	cass_conn.updatePage(tmp_page);
	    	cass_conn.addToQuery(PageCassandra.URLQUERY, System.currentTimeMillis()/1000, 
	    			tmp_page.getUniqKey(), MD5Signature.getMD5(tmp_page.getDomainName()));
    	}
 
        _collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("originPage", "newLinks"));
    }
	
}
