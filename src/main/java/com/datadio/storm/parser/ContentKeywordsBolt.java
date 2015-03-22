package com.datadio.storm.parser;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.datadio.storm.lib.DioConfig;
import com.datadio.storm.lib.KeywordsParser;
import com.datadio.storm.lib.DioSQL;
import com.datadio.storm.lib.WebPage;


public class ContentKeywordsBolt extends BaseRichBolt {

	private static final long serialVersionUID = 8299700136214116161L;
	
	OutputCollector _collector;
	private final String sqlHost;
	private final String sqlDatabase;
	private final String sqlUsername;
	private final String sqlPassword;
	DioSQL sql_conn = null;
	Map<Integer, List<String>> term_container;
	private static final Logger LOG = LoggerFactory.getLogger(ContentKeywordsBolt.class);
	
	public ContentKeywordsBolt(DioConfig config) {
		sqlHost = (String) config.get("sql:url");
		sqlDatabase = (String) config.get("sql:database");
		sqlUsername = (String) config.get("sql:username");
		sqlPassword = (String) config.get("sql:password");
	}

    @SuppressWarnings("rawtypes")
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
//    	System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Connecting SQL");
    	_collector = collector;
//    	this.sql_conn = new DioSQL("jdbc:mysql://localhost:3306", "socialmon_develop_test", "root", "abc123");
//    	this.sql_conn = new DioSQL("jdbc:mysql://10.0.0.10:3306", "datadio", "datadio", "j38h78237fg32");
    	this.sql_conn = new DioSQL(sqlHost, sqlDatabase, sqlUsername, sqlPassword);
    	
    	term_container = this.sql_conn.getKeywords(DioSQL.BLOG);
    }

    @SuppressWarnings("unchecked")
	public void execute(Tuple tuple) {
    	
    	// refresh term_container
    	if (isTickTuple(tuple)) {
    		
    		Map<Integer, List<String>> updated_term_container = this.sql_conn.getKeywordsForTwitter();
    		
    		if(updated_term_container != null) {
    			term_container = updated_term_container;
    		}
    		
        } else {
//        	WebPage page = (WebPage)tuple.getValue(0);
        	WebPage page = new WebPage((Map<String, Object>)tuple.getValue(0));
    		
    		KeywordsParser kwd_parser = new KeywordsParser(term_container);
    		
    		if(page.getMainContent() != null) {
    			Map<Integer, List<String>> matched_projects = kwd_parser.get_matched_projects(page.getMainContent());
    			
        		if(matched_projects != null && !matched_projects.isEmpty()) {
        			
        			Map<Integer, Integer> total_kwds_container = kwd_parser.get_total_keywords_count();
        			
        			_collector.emit(tuple, new Values(Collections.unmodifiableMap(page), matched_projects, total_kwds_container));
        		}
    		}
    		
//        	_collector.emit(tuple, new Values("1","2","3"));
        }
    	
        _collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("webpage", "matched_keywords", "total_keywords_count"));
    }
    
    private static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
            && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }
}