package com.datadio.storm.fetcher;

import com.datadio.storm.lib.WebPage;
import com.sun.syndication.feed.synd.SyndContent;
import com.sun.syndication.feed.synd.SyndEntry;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.io.FeedException;
import com.sun.syndication.io.SyndFeedInput;
import com.sun.syndication.io.XmlReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang.StringEscapeUtils.unescapeHtml;

import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RSSFetcher {
	
	DioHttpClient rssClient;
	SyndFeedInput feedInput;
	List<WebPage> feedContainer;
	List<String> newLinks;
	private static final Logger LOG = LoggerFactory.getLogger(RSSFetcher.class);
	
	public RSSFetcher() {
		rssClient = new DioHttpClient();
		feedInput = new SyndFeedInput();
		feedContainer = new ArrayList<WebPage>();
		newLinks = new ArrayList<String>();
	}
	
	public byte[] getDoc(String url) {
		try {
			return rssClient.exec(url, "DioFeed");
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
			return null;
		} catch (URISyntaxException e) {
			LOG.error(e.getMessage(), e);
			return null;
		}
	}
	
	public static List<WebPage> getFeedList(byte[] content) {
		List<WebPage> pages = new ArrayList<WebPage>();
		SyndFeedInput feedInput = new SyndFeedInput();
		try {
			SyndFeed feed = feedInput.build(new XmlReader((InputStream)(new ByteArrayInputStream(content)))); 
	        Iterator itEntries = feed.getEntries().iterator();
	        // Just use Regex to find out the generator. 
	        // Not worth fucking with Rome for that at this point
	        // if it's going to make us generate another instance or sub-module 
	        // which'll take more ram when we can just use a static regex.
	        while (itEntries.hasNext()) {  
	        	SyndEntry entry = (SyndEntry) itEntries.next();
	        	
	        	if(entry.getLink() == null) {
	        		continue;
	        	}
	   
	        	WebPage rf = new WebPage(entry.getLink());
	        	
	        	try {
	        		rf.setAuthor(entry.getAuthor());
				} catch (Exception e) {}
	        	
	        	try {
	        		rf.setPublishedDate(entry.getPublishedDate().getTime()/1000);
				} catch (Exception e) {}
	        	
//        		rf.setUrl(entry.getLink());
	                
	            if (entry.getContents() != null) {
	            	StringBuilder sb = new StringBuilder();
	                for (Iterator<?> cit = entry.getContents().iterator(); cit.hasNext();) {
	                    SyndContent syndContent = (SyndContent) cit.next();
	                    String value = syndContent.getValue();
	                    sb.append(value);
	                }
	                rf.setMainContent(sb.toString());
	            }
	            
	            try {
	            	rf.setTitle(unescapeHtml(entry.getTitle()));
				} catch (Exception e) {}
	                
	            try {
	            	rf.setDescription(unescapeHtml(entry.getDescription().getValue()));
	            } catch(Exception e) {}
	            
	            pages.add(rf);
	        }
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			LOG.debug(new String(content));
		}
		
		return pages;
	}
	
	public void fetchFeeds(byte[] content) {
		try {
			SyndFeed feed = feedInput.build(new XmlReader((InputStream)(new ByteArrayInputStream(content)))); 
	        Iterator itEntries = feed.getEntries().iterator();  
	            
	        // Just use Regex to find out the generator. 
	        // Not worth fucking with Rome for that at this point
	        // if it's going to make us generate another instance or sub-module 
	        // which'll take more ram when we can just use a static regex.
	        while (itEntries.hasNext()) {  
	        	SyndEntry entry = (SyndEntry) itEntries.next();
	        	
	        	if(entry.getLink() == null) {
	        		continue;
	        	}
	   
	        	WebPage rf = new WebPage(entry.getLink());
	        	rf.setAuthor(entry.getAuthor());
//        		rf.setUrl(entry.getLink());
	        	rf.setPublishedDate(entry.getPublishedDate().getTime()/1000);
	            
	            if (entry.getContents() != null) {
	            	StringBuilder sb = new StringBuilder();
	                for (Iterator<?> cit = entry.getContents().iterator(); cit.hasNext();) {
	                    SyndContent syndContent = (SyndContent) cit.next();
	                    String value = syndContent.getValue();
	                    sb.append(value);
	                }
	                rf.setMainContent(sb.toString());
	            }
	                
	            try {
	            	rf.setTitle(unescapeHtml(entry.getTitle()));
	            	rf.setDescription(unescapeHtml(entry.getDescription().getValue()));
	               
	            } catch(Exception e) {
	            	LOG.error(e.getMessage(), e);
	            }
	            
	            newLinks.add(entry.getLink());
	            feedContainer.add(rf);
	        }
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
	}
	
	public void fetchFeeds(String url){
		byte[] urlDoc = getDoc(url);
		fetchFeeds(urlDoc);
	}
	
	public List<WebPage> getFeedList() {
		return feedContainer;
	}
	
	public List<String> getNewLinks() {
		return newLinks;
	}
    
    public static void main(String[] args) {
    	
    	String url = "http://www.tanteifile.com/rss/index.rdf";
    	
    	RSSFetcher fetcher = new RSSFetcher();
    	
    	fetcher.fetchFeeds(url);
    	
    	List<WebPage> feeds = fetcher.getFeedList();
    	
    	for(WebPage feed : feeds) {
    		 System.out.println("Title: " + feed.getTitle());  
             System.out.println("Link: " + feed.getUrl());  
             System.out.println("Author: " + feed.getAuthor());  
             System.out.println("Publish Date: " + feed.getPublishedDate());
             System.out.println("Description: " + feed.getDescription());
             System.out.println("Content:");
             System.out.println(feed.getMainContent());
             System.out.println();
    	}
    	
    	List<String> newLinks = fetcher.getNewLinks();
    	System.out.println("New links:");
    	for(String newLink : newLinks) {
    		System.out.println(newLink);
    	}
    	
    }
}