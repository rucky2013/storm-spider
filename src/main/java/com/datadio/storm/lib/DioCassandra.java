package com.datadio.storm.lib;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.*;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.exceptions.HectorException;

//import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
//import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
//import me.prettyprint.cassandra.serializers.IntegerSerializer;
//import me.prettyprint.cassandra.service.ThriftCfDef;
//import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
//import me.prettyprint.hector.api.query.QueryResult;
//import me.prettyprint.hector.api.query.RangeSlicesQuery;










import org.json.simple.JSONObject;

public class DioCassandra {
    
    private final Cluster cluster;
    private final Keyspace ksp;
        
//    private static final ColumnFamilyTemplate<String, String> blogTemplate =
//            new ThriftColumnFamilyTemplate<String, String>( ksp,
//                                                         	"Blog",
//                                                         	StringSerializer.get(),
//                                                     		StringSerializer.get());
    
    private final ColumnFamilyTemplate<String, String> tweetTemplate;
    private final ColumnFamilyTemplate<String, String> entryTemplate;
    private final ColumnFamilyTemplate<String, String> blogTemplate;
    
    public DioCassandra() {
    	
    	 this("127.0.0.1:9160");
    }
    
    public DioCassandra(String urls) {
    	 CassandraHostConfigurator cass_conf = new CassandraHostConfigurator(urls);
    	 cass_conf.setRetryDownedHostsDelayInSeconds(1);
    	 cass_conf.setMaxActive(5);
	   	 cluster = HFactory.getOrCreateCluster("datadio", cass_conf);
	   	 ksp = HFactory.createKeyspace("datadio", cluster);
	   	 tweetTemplate = 
	   			 new ThriftColumnFamilyTemplate<String, String>( ksp,
		                                                           "Tweet",
		                                                           StringSerializer.get(),
		                                                           StringSerializer.get());
	   	 blogTemplate = 
	   			 new ThriftColumnFamilyTemplate<String, String>( ksp,
		                                                           "Blog",
		                                                           StringSerializer.get(),
		                                                           StringSerializer.get());
	   	 entryTemplate =
	   	    		new ThriftColumnFamilyTemplate<String, String>( ksp,
	   	                                                         	"Entry",
	   	                                                         	StringSerializer.get(),
	   	                                                         	StringSerializer.get());
    }
    
    public void insertTweetItem(JSONObject payload) {
    	
        ColumnFamilyUpdater<String, String> feedUpdater = tweetTemplate.createUpdater((String)payload.get("tweet_id"));
        		
        //String user_id = (String)payload.get("userid");
        
        String user_id = String.valueOf(payload.get("userid"));
        
        if(user_id == null) {
        	user_id = "NA-" + (String)payload.get("username");
        }
        		
        feedUpdater.setString("pid", user_id);
        feedUpdater.setString("body", (String)payload.get("tweet"));
        feedUpdater.setString("time", String.valueOf(payload.get("tweeted_at")));
        
        //feedUpdater.setString("lang", (String)payload.get("lang"));
        feedUpdater.setString("lang", "en");
        
        try {
        	tweetTemplate.update(feedUpdater);
//            System.out.println("Insert completed to Tweet => " + payload.get("tweet_id"));
        } catch (HectorException e) {
            System.out.println("********** FAILED UPDATE FOR NEW TWEET ITEM => " + payload.get("tweet_id"));
            e.printStackTrace();
        }                                                        
        
    }
    
    @SuppressWarnings("unchecked")
	public String insertTweetEntry(JSONObject payload) throws UnsupportedEncodingException {
        
    	String pid = String.valueOf(payload.get("project_id"));
    	String timestamp = String.valueOf(payload.get("tweeted_at")); 
    	String iid = (String)payload.get("item_id");
    	
    	// couldn't get tweet_id which is wired.
    	if(iid == null || pid == null) return null;
    	
    	List<String> tags_array = (ArrayList<String>) payload.get("tags_array");
    	List<String> lists_array = (ArrayList<String>) payload.get("lists_array");
    	
    	String entryID = MD5Signature.getMD5(pid + "T" + timestamp + iid);
    	
        ColumnFamilyUpdater<String, String> entryUpdater = entryTemplate.createUpdater(entryID);
        
        entryUpdater.setString("_id", entryID);
        entryUpdater.setString("pid", pid);
        entryUpdater.setString("iid", iid);
        entryUpdater.setString("score", (String)payload.get("score"));
        entryUpdater.setString("t", "T");
        entryUpdater.setString("cat", timestamp);
        
        if(payload.get("sentiment") != null) {
//        	System.out.println("$$$$$$$$$$$$$$$$$$======" + String.valueOf(payload.get("sentiment")));
        	entryUpdater.setString("s", String.valueOf(payload.get("sentiment")));
        	entryUpdater.setString("pos", String.valueOf(payload.get("sent_pos")));
        	entryUpdater.setString("neg", String.valueOf(payload.get("sent_neg")));
        }
	    
        for(String keyword : tags_array) {
        	entryUpdater.setString("TA_" + URLEncoder.encode(keyword, "UTF-8"), "1");
        }
          
        for(String list : lists_array) {
        	if(!list.equals("Tweet") && !list.equals("Blog") && !list.equals("Forum")) {
        		entryUpdater.setString("LI_" + URLEncoder.encode(list, "UTF-8"), "1");
        	}
        }
          
        try {
            entryTemplate.update(entryUpdater);
        } catch (HectorException e) {
            System.out.println("********** FAILED UPDATE FOR NEW ENTRY ID => " + entryID);
            e.printStackTrace();
        }
        
        return entryID;
    }
    
    public void insertBlogItem(WebPage article, String lang) {
        // We need to have the hash of the parent domain as well as the hash for the feed URL as well.
        //   This is through -> md5(article.domain())
        
        ColumnFamilyUpdater<String, String> feedUpdater = blogTemplate.createUpdater(MD5Signature.getMD5(article.getCleanedUrl()));
        feedUpdater.setString("title", String.valueOf(article.getTitle()));
        feedUpdater.setString("url", String.valueOf(article.getCleanedUrl()));
        feedUpdater.setString("body", String.valueOf(article.getMainContent()));
        feedUpdater.setString("pid", MD5Signature.getMD5(article.getDomainName()));
        feedUpdater.setString("time", Long.toString(article.getPublishedDate()));
        feedUpdater.setString("lang", lang);
        
        try {
            blogTemplate.update(feedUpdater);
            System.out.println("Insert completed to Blog for Parent Domain => " + MD5Signature.getMD5(article.getDomainName()));
        } catch (HectorException e) {
            System.out.println("********** FAILED UPDATE FOR NEW BLOG ITEM => " + article.getCleanedUrl());
            e.printStackTrace();
        }                                                        
        
    }
    
    public void insertBlogEntry(Integer pid, String iid, Integer score, Long timeframe, List<String> keywords) throws UnsupportedEncodingException {
        // We need to have the hash of the parent domain as well as the hash for the feed URL as well.
        //   This is through -> md5(article.domain())
        
        String entryID = MD5Signature.getMD5(pid + "B" + timeframe + iid);
        
        System.out.println("About to Insert Blog Item to \"Entry\" CF, ID => " + entryID);
        
        ColumnFamilyUpdater<String, String> entryUpdater = entryTemplate.createUpdater(entryID);
          entryUpdater.setString("_id", entryID);
          entryUpdater.setString("pid", Integer.toString(pid));
          entryUpdater.setString("iid", iid);
          entryUpdater.setString("cat", Long.toString(timeframe/1000));
          entryUpdater.setString("t", "B");
          entryUpdater.setString("score", Integer.toString(score));
          
          for(String keyword : keywords) {
        	  entryUpdater.setString("TA_" + URLEncoder.encode(keyword, "UTF-8"), "1");
        	  System.out.println("Key for Cassandra to Add -> TA_" + URLEncoder.encode(keyword, "UTF-8"));
          }
          
          // Encoder for tags/lists/etc -> URLEncoder.encode
          //entryUpdater.setString("s", iid);     // Sentiment Score
          //entryUpdater.setString("pos", iid);   // Positive Sentiment Score
          //entryUpdater.setString("neg", iid);   // Negative Sentiment Score
        
        try {
            entryTemplate.update(entryUpdater);
        } catch (HectorException e) {
            System.out.println("********** FAILED UPDATE FOR NEW ENTRY ID => " + entryID);
            e.printStackTrace();
        }
        
    }
    
    
    public Map<String, Boolean> containsKeys(List<String> keysList, String columnFamilyName) {
    	MultigetSliceQuery<String, String, String> mutliPagesQuery =
    		    HFactory.createMultigetSliceQuery(ksp, StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
    	mutliPagesQuery.setColumnFamily(columnFamilyName);
    	mutliPagesQuery.setKeys(keysList);
    	mutliPagesQuery.setRange(null, null, false, 1);
    	QueryResult<Rows<String, String, String>> result = mutliPagesQuery.execute();
    	Rows<String, String, String> orderedRows = result.get();
    	
    	Map<String, Boolean> re = new HashMap<String, Boolean>();
    	for (Row<String, String, String> r : orderedRows) {
//            System.out.println("Key: " + r.getKey() + " ---- Columns:");
            
            ColumnSlice<String, String> slice = r.getColumnSlice();
            
            WebPage webPage = new WebPage();
            
            webPage.setUniqKey(r.getKey());
            
            if(slice.getColumns() == null || slice.getColumns().isEmpty()) {
            	re.put(r.getKey(), false);
            } else {
            	re.put(r.getKey(), true);
            }
    	}
    	
    	return re;
    }
    
    public ByteBuffer toByteBuffer(Integer obj) {
        if (obj == null) {
        return null;
        }
        int l = obj;
        int size = 4;
        byte[] b = new byte[size];
        for (int i = 0; i < size; ++i) {
        b[i] = (byte) (l >> (size - i - 1 << 3));
        }
        return ByteBuffer.wrap(b);
    }
    
    public String byteBufferToString(ByteBuffer s) {
        byte[] bytearr = new byte[s.remaining()];
        s.get(bytearr);
        String f = new String(bytearr);
        return f;
        
    }
    
    public static void main(String[] args) throws Exception {
    	List<String> keysList = new ArrayList<String>();
    	keysList.add("cc39edba59e3c1c29682afd025b01b66");
    	keysList.add("BB39edba59e3c1c29682afd025b01b67");
    	DioCassandra cass = new DioCassandra();
    	Map<String, Boolean> mp = cass.containsKeys(keysList, "Blog");
    	for(Map.Entry<String, Boolean> entry : mp.entrySet()) {
    		System.out.println("Key: " + entry.getKey() + " exists? " + entry.getValue());
    	}
    	
    }
}
