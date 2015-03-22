package com.datadio.storm.lib;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.json.simple.JSONObject;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

public class DioRedis {
	private final JedisPool redis_pool;
	
	public DioRedis() {
		this("localhost");
	}
	
	public DioRedis(String url) {
		JedisPoolConfig redis_conf = new JedisPoolConfig();
		redis_conf.setMaxActive(5);
		redis_pool = new JedisPool(redis_conf, url);
	}
	
	@SuppressWarnings("unchecked")
	public void aggregate_record(String entry_id, JSONObject payload) throws UnsupportedEncodingException {
		
    	String project_id = String.valueOf(payload.get("project_id"));
    	List<String> tags_array = (ArrayList<String>) payload.get("tags_array");
    	List<String> lists_array = (ArrayList<String>) payload.get("lists_array");
    	
		int time_now = (int) (System.currentTimeMillis() / 1000L);
		
		SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat monthDate = new SimpleDateFormat("yyyy-MM");
		
		Date now = new Date();
		String today = sdfDate.format(now);
		Jedis jedis_conn = redis_pool.getResource();
		
    	try {
    		
    		Pipeline redis_pip = jedis_conn.pipelined();
    		
    		for (String list : lists_array) {
    			String list_escape = URLEncoder.encode(list, "UTF-8");
    			String list_name = project_id + ":LI:" + list_escape;
    			String list_counter_name = project_id + ":NCLI:" + list_escape + ":" + today;
    			
//    			jedis_conn.zadd(list_name, time_now, entry_id);
//    			jedis_conn.incr(list_counter_name);
//    			jedis_conn.zremrangeByRank(list_name, 0, -1001);
    			
    			redis_pip.zadd(list_name, time_now, entry_id);
    			redis_pip.incr(list_counter_name);
    			redis_pip.zremrangeByRank(list_name, 0, -1001);
			}
    		
    		for (String tag : tags_array) {
    			String tag_escape = URLEncoder.encode(tag, "UTF-8");
    			String tag_name = project_id + ":TA:" + tag_escape;
    			String tag_counter_name = project_id + ":NCTA:" + tag_escape + ":" + today;
    			
//    			jedis_conn.zadd(tag_name, time_now, entry_id);
//    			jedis_conn.incr(tag_counter_name);
//    			jedis_conn.zremrangeByRank(tag_name, 0, -1001);
    			
    			redis_pip.zadd(tag_name, time_now, entry_id);
    			redis_pip.incr(tag_counter_name);
    			redis_pip.zremrangeByRank(tag_name, 0, -1001);
    			
    			if(payload.get("sentiment") != null) {
    				String senti_tag = project_id + ":NCST:" + tag_escape + ":" + String.valueOf(payload.get("sentiment")) + ":" + today;
//    				jedis_conn.incr(senti_tag);
    				redis_pip.incr(senti_tag);
    			}
    			
    			String entry_type = (String)payload.get("item_type");
    			String tag_by_type = project_id + ":NCTATP:" + entry_type + ":" + tag_escape + ":" + today;
//    			jedis_conn.incr(tag_by_type);
    			redis_pip.incr(tag_by_type);
    		}
    		
    		if(payload.get("sentiment") != null) {
    			String senti = String.valueOf(payload.get("sentiment"));
    			String senti_name = project_id + ":" + senti;
    			String senti_counter_name = project_id + ":NCST:" + senti + ":" + today;
    			
//    			jedis_conn.zadd(senti_name, time_now, entry_id);
//    			jedis_conn.incr(senti_counter_name);
//    			jedis_conn.zremrangeByRank(senti_name, 0, -1001);
    			redis_pip.zadd(senti_name, time_now, entry_id);
    			redis_pip.incr(senti_counter_name);
    			redis_pip.zremrangeByRank(senti_name, 0, -1001);
    		}
    		
//    		jedis_conn.zadd(project_id, time_now, entry_id);
//    		jedis_conn.zadd(project_id + ":TW", time_now, entry_id);
//    		
//    		jedis_conn.incr(project_id+":NCT:"+today);
//    		jedis_conn.incr(project_id+":NCT:TW:"+today);
//    		
//    		// total notification monthly count
//    		jedis_conn.incr(project_id+":TNC:"+monthDate.format(now));
//    		
//    		jedis_conn.zremrangeByRank(project_id, 0, -1001);
//    		jedis_conn.zremrangeByRank(project_id + ":TW", 0, -1001);
    		
    		redis_pip.zadd(project_id, time_now, entry_id);
    		redis_pip.zadd(project_id + ":TW", time_now, entry_id);
    		
    		redis_pip.incr(project_id+":NCT:"+today);
    		redis_pip.incr(project_id+":NCT:TW:"+today);
    		
    		// total notification monthly count
    		redis_pip.incr(project_id+":TNC:"+monthDate.format(now));
    		
    		redis_pip.zremrangeByRank(project_id, 0, -1001);
    		redis_pip.zremrangeByRank(project_id + ":TW", 0, -1001);
			
    		redis_pip.sync();
		}  finally {
			redis_pool.returnResource(jedis_conn);
		}
	}
	
	public void aggregate_blog_record(WebPage page, Integer pid, List<String> keywords) throws UnsupportedEncodingException {
		
		Jedis jedis_conn = redis_pool.getResource();
		long postTimestamp = page.getPublishedDate()/1000;
		String itemID = MD5Signature.getMD5(page.getCleanedUrl());
        String entryID = MD5Signature.getMD5(pid + "B" + postTimestamp + itemID);
        
        try {
        	Pipeline redis_pip = jedis_conn.pipelined();
            // This is where we add the info to Redis
            // And hopefully we don't forget to zremrangebyrank on the ID lists
            //   to keep them no more than 1000 entries.
            
            // We'll not only need to add the ID to the general project but also:
            
            long epoch = System.currentTimeMillis()/1000;
            //System.out.println("Current Timestamp: " + epoch);
            SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd");
            SimpleDateFormat yrMo = new SimpleDateFormat("yyyy-MM-dd");
            Date now = new Date();
            String strDate = sdfDate.format(now);
            
            System.out.println("Adding to Redis...");
            System.out.println("****** Adding Entry ID:" + entryID);
            
            redis_pip.zadd(Integer.toString(pid), epoch, entryID);
            redis_pip.zadd(Integer.toString(pid) + ":BL", epoch, entryID);
            
            redis_pip.zremrangeByRank(Integer.toString(pid), 0, -1001);
            redis_pip.zremrangeByRank(Integer.toString(pid) + ":BL", 0, -1001);
            
            // Notification Counts
            redis_pip.incr(Integer.toString(pid) + ":NCT:" + strDate);
            redis_pip.incr(Integer.toString(pid) + ":NC:BL:" + strDate);
            redis_pip.incr(Integer.toString(pid) + ":TNC:" + yrMo.format(now));
            
            // Add to the "Blog" list
            redis_pip.zadd(Integer.toString(pid) + ":LI:Blog", epoch, entryID);
            redis_pip.incr(Integer.toString(pid) + ":NCLI:Blog:" + strDate);
            redis_pip.zremrangeByRank(Integer.toString(pid) + ":LI:Blog", 0, -1001);
            
            
            // Add to the Sentiment list when we do it.
            
            // Add the ID's to the lists for the keywords (tags array) & Increment the Counts
            for(String keyword : keywords) {
            	redis_pip.zadd(Integer.toString(pid) + ":TA:" + URLEncoder.encode(keyword, "UTF-8"), epoch, entryID);
            	redis_pip.incr(Integer.toString(pid) + ":NCTA:" + URLEncoder.encode(keyword, "UTF-8") + ":" + strDate);
            	redis_pip.incr(Integer.toString(pid) + ":NCTATP:Blog:" + URLEncoder.encode(keyword, "UTF-8") + ":" + strDate);
            	redis_pip.zremrangeByRank(Integer.toString(pid) + ":TA:" + URLEncoder.encode(keyword, "UTF-8"), 0, -1001);
            }
            
            redis_pip.sync();
            
        } catch(Exception e) {
            System.out.println("Caught Exception on Redis");
            e.printStackTrace();
        } finally {
			redis_pool.returnResource(jedis_conn);
		}
	}
	
	public void update_notifications(Integer company_id, String item_type){
		Jedis jedis_conn = redis_pool.getResource();
		
		try {
			// Company Billing Notifications
			jedis_conn.incr("CBN:" + company_id.toString());
			// Company billing circle wised notification count
			jedis_conn.incr("CBN:" + company_id.toString() + ":" + item_type);
		}  finally {
			redis_pool.returnResource(jedis_conn);
		}
	}	
}
