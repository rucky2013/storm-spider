package com.datadio.storm.storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datadio.storm.lib.MD5Signature;
import com.datadio.storm.lib.WebPage;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.*;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.exceptions.HectorException;


public class PageCassandra {
    
    private final Cluster cluster;
    private final Keyspace ksp;
    private final ColumnFamilyTemplate<String, String> pageTemplate;
    private final ColumnFamilyTemplate<String, String> feedTemplate;
    private final ColumnFamilyTemplate<String, String> siteTemplate;
    private final ColumnFamilyTemplate<String, String> pageQueryTemplate;
    private final ColumnFamilyTemplate<String, String> rssQueryTemplate;
    
    public final static int URLQUERY = 0;
    public final static int RSSQUERY = 1;
    
    private final static int columnLength = 20;
    
    private static final Logger LOG = LoggerFactory.getLogger(PageCassandra.class);
    
    public PageCassandra() {
    	
    	 this("127.0.0.1:9160");
    }
    
    public PageCassandra(String urls) {
    	 CassandraHostConfigurator cass_conf = new CassandraHostConfigurator(urls);
    	 cass_conf.setRetryDownedHostsDelayInSeconds(1);
    	 cass_conf.setMaxActive(5);
	   	 cluster = HFactory.getOrCreateCluster("datadio", cass_conf);
	   	 ksp = HFactory.createKeyspace("Crawl", cluster);
	   	
	   	 pageTemplate =
	   	    	 new ThriftColumnFamilyTemplate<String, String>( ksp,
	   	                                                         	"Page",
	   	                                                         	StringSerializer.get(),
	   	                                                         	StringSerializer.get());
	   	feedTemplate =
	             new ThriftColumnFamilyTemplate<String, String>(ksp,
	                                                            "Feed",
	                                                            StringSerializer.get(),
	                                                            StringSerializer.get());
	   	siteTemplate =
	            new ThriftColumnFamilyTemplate<String, String>(ksp,
	                                                           "Site",
	                                                           StringSerializer.get(),
	                                                           StringSerializer.get());
	   	
	   	pageQueryTemplate = 
	   			 new ThriftColumnFamilyTemplate<String, String>( ksp,
		                                                           "PageQuery",
		                                                           StringSerializer.get(),
		                                                           StringSerializer.get());
	   	
	   	rssQueryTemplate = 
	   			 new ThriftColumnFamilyTemplate<String, String>( ksp,
		                                                           "RSSQuery",
		                                                           StringSerializer.get(),
		                                                           StringSerializer.get());
    } 
    
    public WebPage getFeed(String feedKey) {
    	ColumnFamilyResult<String, String> res = siteTemplate.queryColumns(feedKey);
    	WebPage page = new WebPage();
    	page.setUrl(res.getString("url"));
    	page.setDomainName(res.getString("d"));
    	page.setFetchTime(Long.parseLong(res.getString("ft")));
    	page.setPrevFetchTime(Long.parseLong(res.getString("pret")));
    	page.setFetchInterval(Integer.parseInt(res.getString("int")));
        
    	return page;
    }
    
    
    public List<WebPage> getFeeds(List<String> pageList) {
    	MultigetSliceQuery<String, String, String> mutliPagesQuery =
    		    HFactory.createMultigetSliceQuery(ksp, StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
    	mutliPagesQuery.setColumnFamily("Feed");
    	mutliPagesQuery.setKeys(pageList);
    	mutliPagesQuery.setRange(null, null, false, columnLength);
    	QueryResult<Rows<String, String, String>> result = mutliPagesQuery.execute();
    	Rows<String, String, String> orderedRows = result.get();
    	
    	List<WebPage> webPages = new ArrayList<WebPage>();
    	
    	for (Row<String, String, String> r : orderedRows) {
//            System.out.println("Key: " + r.getKey() + " ---- Columns:");
            
            ColumnSlice<String, String> slice = r.getColumnSlice();
            
            WebPage webPage = new WebPage();
            
            webPage.setUniqKey(r.getKey());
            
            for (HColumn<String, String> column: slice.getColumns())
            {
//               System.out.println("HColumn: " + column);
               if ("d".equals(column.getName())) // domain name
               {
                  String current_domain = column.getValue();
                  webPage.setDomainName(current_domain);
//                  System.out.println("Found domain. Value: " + current_domain);
               } else if ("url".equals(column.getName())) {
            	  webPage.setUrl(column.getValue()); 
               } else if ("ft".equals(column.getName())) {
            	   webPage.setFetchTime(Long.parseLong(column.getValue()));
               } else if ("pret".equals(column.getName())) {
            	   webPage.setPrevFetchTime(Long.parseLong(column.getValue()));
               } else if ("int".equals(column.getName())) {
            	   webPage.setFetchInterval(Integer.parseInt(column.getValue()));
               }
            }
            webPages.add(webPage);
    	}
    	
    	return webPages;
    }
    
//  public void insertFeed(String parentDomain, String feedURL) {
//  // We need to have the hash of the parent domain as well as the hash for the feed URL as well.
//  
//  String pkey = MD5Signature.getMD5(parentDomain);
//  String feedHash = MD5Signature.getMD5(feedURL);
//  
//  ColumnFamilyUpdater<String, String> feedUpdater = feedTemplate.createUpdater(feedHash);
//  feedUpdater.setString("pkey", pkey);
//  feedUpdater.setString("url", feedURL);
//  feedUpdater.setInteger("last", 0);
//  
//  try {
//      feedTemplate.update(feedUpdater);
//  } catch (HectorException e) {
//      LOG.error("Failed to insert feed: {} into cassandra", feedURL);
//      LOG.error(e.getMessage(), e);
//  }                                                        
//  
//}
//
    
    public void addFeed(WebPage page) {
        // We need to have the hash of the parent domain as well as the hash for the feed URL as well.
        
        String pkey = MD5Signature.getMD5(page.getDomainName());
        
        ColumnFamilyUpdater<String, String> feedUpdater = feedTemplate.createUpdater(page.getUniqKey());
        
        feedUpdater.setString("d", String.valueOf(page.getDomainName()));
        feedUpdater.setString("url", page.getUrl());
        feedUpdater.setString("pkey", pkey);
//        feedUpdater.setInteger("last", 0);
        
        feedUpdater.setString("ft", String.valueOf(page.getFetchTime()));
        feedUpdater.setString("pret", String.valueOf(page.getPrevFetchTime()));
        feedUpdater.setString("int", String.valueOf(page.getFetchInterval()));
        
        try {
            feedTemplate.update(feedUpdater);
        } catch (HectorException e) {
            LOG.error("Failed to add feed: {} into cassandra",  page.getUrl());
            LOG.error(e.getMessage(), e);
        }                                                        
    }
    
    public void updateFeed(WebPage page) {
    	ColumnFamilyUpdater<String, String> feedUpdater = feedTemplate.createUpdater(page.getUniqKey());
    	String pkey = MD5Signature.getMD5(page.getDomainName());
    	
        feedUpdater.setString("d", String.valueOf(page.getDomainName()));
        feedUpdater.setString("url", page.getUrl());
        feedUpdater.setString("pkey", pkey);
        
    	feedUpdater.setString("ft", String.valueOf(page.getFetchTime()));
        feedUpdater.setString("pret", String.valueOf(page.getPrevFetchTime()));
        feedUpdater.setString("int", String.valueOf(page.getFetchInterval()));
        feedUpdater.setString("modt", String.valueOf(page.getModifiedTime()));
        feedUpdater.setString("preModT", String.valueOf(page.getPrevModifiedTime()));
        feedUpdater.setString("lkMD5", String.valueOf(page.getLinkMD5()));
        
        try {
        	pageTemplate.update(feedUpdater);
//            System.out.println("Insert completed to Tweet => " + payload.get("tweet_id"));
        } catch (HectorException e) {
        	LOG.error(e.getMessage(), e);
        }              
    }
    
    public void addSocial(String parentDomain, List<String> sLinks) {
        
        String hash = MD5Signature.getMD5(parentDomain);
        
        ColumnFamilyUpdater<String, String> updater = siteTemplate.createUpdater(hash);
        updater.setString("domain", parentDomain);
        for (String entry : sLinks) {
            updater.setString(entry, "1");
            //System.out.println("Saw " + entry.getKey() + " a total of " + entry.getValue() + " times.");
        }
        
        updater.setInteger("hit", 1);
        try {
            siteTemplate.update(updater);
        } catch (HectorException e) {
            LOG.error(e.getMessage(), e);
        }
        
    }
    
    
    public List<WebPage> getPages(List<String> pageList) {
    	MultigetSliceQuery<String, String, String> mutliPagesQuery =
    		    HFactory.createMultigetSliceQuery(ksp, StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
    	mutliPagesQuery.setColumnFamily("Page");
    	mutliPagesQuery.setKeys(pageList);
    	mutliPagesQuery.setRange(null, null, false, columnLength);
    	QueryResult<Rows<String, String, String>> result = mutliPagesQuery.execute();
    	Rows<String, String, String> orderedRows = result.get();
    	
    	List<WebPage> webPages = new ArrayList<WebPage>();
    	
    	for (Row<String, String, String> r : orderedRows) {
//            System.out.println("Key: " + r.getKey() + " ---- Columns:");
            
            ColumnSlice<String, String> slice = r.getColumnSlice();
            
            WebPage webPage = new WebPage();
            
            webPage.setUniqKey(r.getKey());
            
            for (HColumn<String, String> column: slice.getColumns())
            {
            	String columnName = column.getName();
            	String columnValue = column.getValue();
//               System.out.println("HColumn: " + column);
               if ("d".equals(columnName)) // domain name
               {
                  webPage.setDomainName(columnValue);
                  
               } else if ("url".equals(columnName)) {
            	  webPage.setUrl(columnValue); 
               } else if ("ft".equals(columnName)) {
            	   webPage.setFetchTime(Long.parseLong(columnValue));
               } else if ("pret".equals(columnName)) {
            	   webPage.setPrevFetchTime(Long.parseLong(columnValue));
               } else if ("int".equals(columnName)) {
            	   webPage.setFetchInterval(Integer.parseInt(columnValue));
               } else if ("modt".equals(columnName)) {
            	   webPage.setModifiedTime(Long.parseLong(columnValue));
               } else if ("preModT".equals(columnName)) {
            	   webPage.setPrevModifiedTime(Long.parseLong(columnValue));
               } else if ("lkMD5".equals(columnName)) {
            	   webPage.setLinkMD5(columnValue);
               } else if ("cLen".equals(columnName)) {
            	   webPage.setContentLength(Integer.parseInt(columnValue));
//               } else if ("html".equals(column.getName())) {
//            	   webPage.setRawHtml(column.getValue());
               }
            }
            webPages.add(webPage);
    	}
    	
    	return webPages;
    }
    
    // add new page
    public void addNewPage(WebPage page) {
    	addNewPage(page.getUniqKey(), page);
    }
    
    public void addNewPage(String pageKey, WebPage page) {
    	ColumnFamilyUpdater<String, String> feedUpdater = pageTemplate.createUpdater(pageKey);
		
    	if(page.getUrl() == null || page.getUrl().isEmpty()) {
    		return;
    	}
    	
    	feedUpdater.setString("d", String.valueOf(page.getDomainName()));
    	feedUpdater.setString("url", String.valueOf(page.getUrl()));
        feedUpdater.setString("ft", String.valueOf(page.getFetchTime()));
        feedUpdater.setString("pret", String.valueOf(page.getPrevFetchTime()));
        feedUpdater.setString("int", String.valueOf(page.getFetchInterval()));
        
        try {
        	pageTemplate.update(feedUpdater);
//            System.out.println("Insert completed to Tweet => " + payload.get("tweet_id"));
        } catch (HectorException e) {
        	LOG.error(e.getMessage(), e);
        }          
    }
    
    public void updatePage(WebPage page) {
    	ColumnFamilyUpdater<String, String> feedUpdater = pageTemplate.createUpdater(page.getUniqKey());
    	
    	feedUpdater.setString("d", String.valueOf(page.getDomainName()));
    	feedUpdater.setString("url", String.valueOf(page.getUrl()));
    	feedUpdater.setString("ft", String.valueOf(page.getFetchTime()));
        feedUpdater.setString("pret", String.valueOf(page.getPrevFetchTime()));
        feedUpdater.setString("int", String.valueOf(page.getFetchInterval()));
        feedUpdater.setString("modt", String.valueOf(page.getModifiedTime()));
        feedUpdater.setString("preModT", String.valueOf(page.getPrevModifiedTime()));
        feedUpdater.setString("lkMD5", String.valueOf(page.getLinkMD5()));
        feedUpdater.setString("cLen", String.valueOf(page.getContentLength()));
//        feedUpdater.setString("html", String.valueOf(page.getRawHtml()));
        
        try {
        	pageTemplate.update(feedUpdater);
//            System.out.println("Insert completed to Tweet => " + payload.get("tweet_id"));
        } catch (HectorException e) {
        	LOG.error(e.getMessage(), e);
        }              
    }
    
    public boolean containsPage(WebPage page) {
    	return contains(page.getUniqKey(), "Page");
    }
    
    public boolean containsFeed(WebPage page) {
    	return contains(page.getUniqKey(), "Feed");
    }
    
    private boolean contains(String key, String CFName) {
    	SliceQuery<String, String, String> sliceQuery = HFactory.createSliceQuery(ksp, StringSerializer.get(), 
    			StringSerializer.get(), StringSerializer.get());
    	sliceQuery.setColumnFamily(CFName);
    	sliceQuery.setKey(key);
    	sliceQuery.setRange(null, null, false, 1);
    	QueryResult<ColumnSlice<String, String>> result = sliceQuery.execute();
    	if (result != null && result.get() != null) {
    		return true;
    	} else {
    		return false;
    	}
    }
    
    /**
     * Added url into query column family in cassandra waiting to be re-crawled
     * @param CFNameCode : the int that stands for the column family name
     * @param timestamp : timestamp as the row key
     * @param pageKey : the md5 of the url as column name
     * @param pageDomainKey : the md5 of the domain of the url as the value of the column
     */
    public void addToQuery(int CFNameCode, long timestamp, String pageKey, String pageDomainKey) {
    	long shortStamp = timestamp / 1000;
    	ColumnFamilyTemplate<String, String> temp;
    	
    	switch (CFNameCode) {
		case PageCassandra.URLQUERY:
			temp = pageQueryTemplate;
			break;
		case PageCassandra.RSSQUERY:
			temp = rssQueryTemplate;
			break;
		default:
			temp = pageQueryTemplate;
			break;
		}
    	
//    	System.out.println("@@@@ Added " + pageKey + " to " + shortStamp + " in CF " + CFNameCode);
    	
    	ColumnFamilyUpdater<String, String> feedUpdater = temp.createUpdater(Long.toString(shortStamp));
    	
    	feedUpdater.setString(pageKey, pageDomainKey);
    	
    	try {
    		temp.update(feedUpdater);
//            System.out.println("Insert completed to Tweet => " + payload.get("tweet_id"));
        } catch (HectorException e) {
        	LOG.error(e.getMessage(), e);
        }         
    }
    
	//  public void getDomainFromSite
	    
	public Map<String, String> getNextQuery(int CFNameCode, long timeKey) {
	  	Map<String, String> page_keys = new HashMap<String, String>();
	  	String cfName;
	  	
	  	switch (CFNameCode) {
		case PageCassandra.URLQUERY:
			cfName = "PageQuery";
			break;
		case PageCassandra.RSSQUERY:
			cfName = "RSSQuery";
			break;
		default:
			cfName = "PageQuery";
			break;
		}
	  	
	  	SliceQuery<String, String, String> page_query = HFactory.createSliceQuery(ksp, StringSerializer.get(),
	  		    StringSerializer.get(), StringSerializer.get()).
	  		    setKey(Long.toString(timeKey)).setColumnFamily(cfName);
	  	
	  	ColumnSliceIterator<String, String, String> iterator =
	  		    new ColumnSliceIterator<String, String, String>(page_query, null, "\uFFFF", false);
	
	  	while (iterator.hasNext()) {
	  		HColumn<String, String> thisColumn = iterator.next();
	  		
	  		page_keys.put(thisColumn.getName(), thisColumn.getValue());
	  	}
	  	
	  	return page_keys;
	}
	
	public void deleteNullUrlRows() {
		Set<String> nullKeys = findAllNull("Page");
		if(nullKeys!=null && !nullKeys.isEmpty()) {
			System.out.println("Found totally: " + nullKeys.size() + " null Pages.");
			deleteRows(nullKeys, "Page");
		}
	}
	
	private void deleteRows(Collection<String> keys, String columnFamily) {
		Mutator<String> cmutator = HFactory.createMutator(ksp, StringSerializer.get());
		
		for(String key : keys) {
			cmutator.addDeletion(key, columnFamily);
		}
		
		cmutator.execute();
	}
	
	private Set<String> findAllNull(String columnFamilyName) {
	    Map<String, Integer> resultMap = new HashMap<String, Integer>();
	    Set<String> nullKeys = new HashSet<String>();
	    String lastKeyForMissing = "";
	    StringSerializer s = StringSerializer.get();
	    RangeSlicesQuery<String, String, String> allRowsQuery = HFactory.createRangeSlicesQuery(ksp, s, s, s);
	    allRowsQuery.setColumnFamily(columnFamilyName);
	    allRowsQuery.setRange("", "", false, columnLength);
	    //allRowsQuery.setReturnKeysOnly();    //enable this line if we want key only
	    allRowsQuery.setRowCount(100);
	    int rowCnt = 0;
	    while (true) {
	        allRowsQuery.setKeys(lastKeyForMissing, "");
	        QueryResult<OrderedRows<String, String, String>> res = allRowsQuery.execute();
	        OrderedRows<String, String, String> rows = res.get();
	        lastKeyForMissing = rows.peekLast().getKey();
	        for (Row<String, String, String> aRow : rows) {
	            if (!resultMap.containsKey(aRow.getKey())) {    
	                resultMap.put(aRow.getKey(), ++rowCnt);
	                System.out.println(aRow.getKey() + ":" + rowCnt);
	            }
	            
	            ColumnSlice<String, String> slice = aRow.getColumnSlice();
	            HColumn<String, String> column = slice.getColumnByName("url");
	            if(column != null) {
		            if( column.getValue() == null || column.getValue().isEmpty() ) {
		            	System.out.println(">>> Found key with empty url");
		            	nullKeys.add(aRow.getKey());
		            }
	            }
	            
	        }
	        if (rows.getCount() != 100) {
	            //end of the column family
	            break;
	        }
	    }
	    return nullKeys;
	}
    
    private ByteBuffer toByteBuffer(Integer obj) {
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
    
    private String byteBufferToString(ByteBuffer s) {
        byte[] bytearr = new byte[s.remaining()];
        s.get(bytearr);
        String f = new String(bytearr);
        return f;
        
    }
    
    // test stuff
    public static void main(String[] args) throws Exception {
//    	PageCassandra cass = new PageCassandra();
//    	WebPage webPageTest = new WebPage();
//    	WebPage webPageTest2 = new WebPage();
//    	WebPage webPageTest3 = new WebPage();
//    	WebPage webPageTest4 = new WebPage();
//    	List<String> pageKeys = new ArrayList<String>();
//    
//    	webPageTest.setDomainName("cnn.com");
//    	webPageTest.setUrl("http://www.cnn.com");
//    	webPageTest.setFetchInterval(2);
//    	
//    	pageKeys.add(webPageTest.getUniqKey());
//    	cass.addPage(webPageTest);
//    	
//    	webPageTest2.setDomainName("techcrunch.com");
//    	webPageTest2.setUrl("http://www.techcrunch.com/");
//    	pageKeys.add(webPageTest2.getUniqKey());
//    	cass.addPage(webPageTest2);
//    	
//    	webPageTest3.setDomainName("themeforest.net");
//    	webPageTest3.setUrl("http://themeforest.net/category/site-templates");
//    	pageKeys.add(webPageTest3.getUniqKey());
//    	cass.addPage(webPageTest3);
//    	
//    	webPageTest4.setDomainName("huffingtonpost.com");
//    	webPageTest4.setUrl("http://www.huffingtonpost.com");
//    	pageKeys.add(webPageTest4.getUniqKey());
//    	cass.addPage(webPageTest4);
    	
    	
//    	long timestamp = System.currentTimeMillis();
//    	cass.setPageQuery(timestamp, pageKeys);
    	
//    	List<String> pageKeysTest = cass.getPageQuery(timestamp);
//    	
//    	System.out.println("PageQuery key is: " + timestamp);
    	
//    	for(String key : pageKeysTest) {
//    		System.out.println("Get keys from cassandra: " + key);
//    	}
//    	
//    	List<WebPage> webPagesRetrieved = cass.getPages(pageKeysTest);
    	
//    	for(WebPage page : webPagesRetrieved) {
//    		System.out.println("Get page from cassandra: " + page.getUrl());
//    	}
    	
//		PageCassandra cass = new PageCassandra();
//		PageRedis redis_conn = new PageRedis();
//		String[] urls = {
//				"http://www.cnn.com",
//				"http://www.techcrunch.com/",
//				"http://www.huffingtonpost.com",
//				"http://www.washingtonpost.com/",
//				"http://www.nytimes.com"
//		};
//		
//		for(String url : urls) {
//			WebPage newPage = new WebPage(url);
//			System.out.println("Adding: " + url + " : " + newPage.getUniqKey());
//			cass.addNewPage(newPage);
//			redis_conn.addToQueue(newPage, 6000);
//		}
//		
//		System.out.println("Finished!");
//		Map<String, String> keys = cass.getNextQuery(PageCassandra.URLQUERY, 1381152);
//		for(Map.Entry<String, String> key : keys.entrySet()) {
//			System.out.println("Key:" + key.getKey() + " with value:" + key.getValue());
//		}
		
//    	PageCassandra cass = new PageCassandra("10.0.0.152:9160, 10.0.0.154:9160, 10.0.0.150:9160");
//		cass.deleteNullUrlRows();
    }
}

