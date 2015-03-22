package com.datadio.storm.parser;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.datadio.storm.lib.BlogScore;
import com.datadio.storm.lib.DioConfig;
import com.datadio.storm.lib.DioSQL;
import com.datadio.storm.lib.SimpleCache;
import com.datadio.storm.lib.WebPage;


public class ContentScoreBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 2155169253101266L;
	
	OutputCollector _collector;
	private final String sqlHost;
	private final String sqlDatabase;
	private final String sqlUsername;
	private final String sqlPassword;
	DioSQL sql_conn;
	private SimpleCache<Integer, Map<String, List<String>>> clientCache;
	private SimpleCache<Integer, Map<String, List<String>>> competitorCache;
	private int secondsToLive = 300;
	
	public static final Logger LOG = LoggerFactory.getLogger(ContentScoreBolt.class);
	
	public ContentScoreBolt(DioConfig config) {
		sqlHost = (String) config.get("sql:url");
		sqlDatabase = (String) config.get("sql:database");
		sqlUsername = (String) config.get("sql:username");
		sqlPassword = (String) config.get("sql:password");
	}

    @SuppressWarnings("rawtypes")
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    	_collector = collector;
    	this.sql_conn = new DioSQL(sqlHost, sqlDatabase, sqlUsername, sqlPassword);
//    	this.sql_conn = new DioSQL("jdbc:mysql://10.0.0.10:3306", "datadio", "datadio", "j38h78237fg32");
    	clientCache = new SimpleCache<Integer, Map<String, List<String>>>(500);
    	competitorCache = new SimpleCache<Integer, Map<String, List<String>>>(500);
    }

    @SuppressWarnings("unchecked")
	public void execute(Tuple tuple) {
    	
//    	WebPage page = (WebPage)tuple.getValue(0);
    	WebPage page = new WebPage((Map<String, Object>)tuple.getValue(0));
    	Map<Integer, List<String>> matched_projects = (HashMap<Integer, List<String>>)tuple.getValue(1);
    	Map<Integer, Integer> total_kwds_container = (HashMap<Integer, Integer>)tuple.getValue(2);
    	
    	Map<String, List<String>> clients;
    	Map<String, List<String>> competitors;
    	List<String> tagged_kwd;
    	Integer total_kwd_num;
    	
    	for(Integer pid : matched_projects.keySet()) {
    		//System.out.println("======== Received twitter tracked keywords for project" + pid.toString());

    		clients = clientCache.get(pid);
    		if(clients == null) {
    			clients = this.sql_conn.getClients(pid);
    			if(clients != null) {
    				clientCache.put(pid, clients, secondsToLive);
    			}
    		} else {
//    			LOG.info("Find cached client info for project : " + pid);
    		}
    		
    		competitors = competitorCache.get(pid);
    		
    		if(competitors == null) {
    			competitors = this.sql_conn.getCompetitors(pid);
    			if(competitors != null) {
    				competitorCache.put(pid, competitors, secondsToLive);
    			}
    		} else {
//    			LOG.info("Find cached competitor info for project : " + pid);
    		}
			
			tagged_kwd = matched_projects.get(pid);
			total_kwd_num = total_kwds_container.get(pid);
			
			Integer total_score = BlogScore.ScoreItem(clients, competitors, page, null, page.getMainContent(), tagged_kwd.size(), total_kwd_num);
    	
//			System.out.println("****** Score: " + total_score);
//			total_score = tw.get_score(clients_list, competitors_list, total_kwd_num.intValue(), tagged_kwd.size(), tweet_record);
	        page.setScore(total_score);
	        page.setTaggedKeywords(tagged_kwd);
	        
	        _collector.emit(tuple, new Values(Collections.unmodifiableMap(page), pid));
    	}
    	
    	 _collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("webpage", "project_id"));
    }
}