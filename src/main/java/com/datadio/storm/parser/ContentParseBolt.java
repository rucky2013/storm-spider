package com.datadio.storm.parser;

import java.util.Collections;
import java.util.Map;

import com.datadio.storm.fetcher.URLDiscover;
import com.datadio.storm.lib.ForumParser;
import com.datadio.storm.lib.WebPage;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ContentParseBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 1L;
	
	OutputCollector _collector;
	ContentParser parser;

    @SuppressWarnings("rawtypes")
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    	_collector = collector;
    	parser = new ContentParser();
    }

    @SuppressWarnings("unchecked")
	public void execute(Tuple tuple) {
 
//    	WebPage page = (WebPage)tuple.getValue(0);
    	WebPage page = new WebPage((Map<String, Object>)tuple.getValue(0));
    	byte[] contentBytes = (byte[]) tuple.getValue(1);
    	String docString = new String(contentBytes);
    	
    	int contentLength = URLDiscover.getContentLength(docString);
    	
    	// only newly created page is going to parse and score.
    	if(page.getFetchTime() > 0 ) {
    	
	    	// new page or content of the page has been changed
	    	if(	page.getContentLength() == -1 || 
	    		page.getContentLength() != contentLength) {
	    			
				page.setContentLength(contentLength);
				
				if(page.getModifiedTime() > 0) {
					page.setPrevModifiedTime(page.getModifiedTime());
				}
				page.setModifiedTime(System.currentTimeMillis());
				
		    	// detect page type (forum, blog, or regular page)
		    	if(ForumParser.isForum(null, docString)) {
		    		
		    	} else {
		    		String mainContent = parser.extractContent(page.getCleanedUrl(), docString);
		    		
		    		if(mainContent != null) {
	//	    			System.out.println("^^^^ Get content");
		    			page.setMainContent(mainContent);
						_collector.emit(tuple, new Values(Collections.unmodifiableMap(page)));
		    		}
		    	}
			}
    	}
 
        _collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("webPage"));
    }
	
}
