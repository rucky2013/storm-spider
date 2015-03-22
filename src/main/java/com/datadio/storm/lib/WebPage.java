package com.datadio.storm.lib;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import com.datadio.storm.fetcher.URLDiscover;

/**
 * 
 * @author stevenyue
 *	All the time long type of value should be in millis. 
 */
public class WebPage extends HashMap<String, Object> {
//	private String url;
//	private String url_cleaned;
//	private String domainName;
//	private String uniqKey;
//	private String rawHtml;
//	private int status;
//	private long fetchTime;
//	private int fetchInterval;
//	private long modifiedTime;
//	private long prevFetchTime;
//	private long prevModifiedTime;
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(WebPage.class);
	
//	TimeUUIDUtils.getUniqueTimeUUIDinMillis();

	public WebPage() {
		this(null, 0, 0);
	}
	
	public WebPage(String url) {
		this(url, 0, 0);
	}
	
	public WebPage(String url, long fetchTime) {
		this(url, fetchTime, 0);
	}
	
	public WebPage(String url, long fetchTime, int fetchInterval) {
		setUrl(url);
		setFetchTime(fetchTime);
		setFetchInterval(fetchInterval);
	}
	
	public WebPage(Map<String, Object> cMap) {
		for(Map.Entry<String, Object> mapEntry : cMap.entrySet()) {
			put(mapEntry.getKey(), mapEntry.getValue());
		}
	}
	
	private String parseDomain(String url) {
		String domainName = null;
        try {
//        	String newUrl = "http://" + url;
            
        	String[] urlSplited = url.split("/");
        	if(urlSplited.length > 2) {
        		domainName = urlSplited[2];
        		put("domainName", domainName);
        	} else {
        		return null;
        	}
        	
        } catch(Exception e) { 
        	LOG.error("Error'd in getDomain for " + url + " " + this.getUniqKey());
        	LOG.error(e.getMessage(),e);
        	return null;
        }
        
        return domainName;
	}
	
	/**
	 * Generate a MD5 String based on the cleaned url of this page
	 * @return MD5 String
	 */
	public String getUniqKey() {		
		if(containsKey("uniqKey")) {
			return (String)get("uniqKey");
		} else {
			return getMD5();
		}
	}
	
	public void setUniqKey(String key) {
		put("uniqKey", key);
	}
	
	public static String cleanUrl(String url) {
		String url_cleaned;
		url_cleaned = LinkCleaner.getCanonicalURL(url);
		return url_cleaned;
	}
	
	public String getCleanedUrl() {
		
		String url_cleaned;
		
		if(!containsKey("url_cleaned")) {
			// assume url string starts with http://
			url_cleaned = LinkCleaner.getCanonicalURL((String)get("url"));
			put("url_cleaned", url_cleaned);
		} else {
			url_cleaned = (String) get("url_cleaned");
		}
		
		return url_cleaned;
	}
	
	private String getMD5() {
		String url_cleaned = getCleanedUrl();
        if(null == url_cleaned) return null;
        return MD5Signature.getMD5(url_cleaned);
	}
	
	// page, rss, post
	public void setPageType(String type) {
		put("pageType", type);
	}
	
	public String getPageType() {
		return get("pageType") == null ? null : (String)get("pageType");
	}

	public String getUrl() {
		return (String)get("url");
	}
	
	public void setUrl(String url) {
		put("url", url);
	}
	
	public String getDomainName() {
		if(containsKey("domainName")) {
			return (String)get("domainName");
		} else {
			if(get("url") != null) {
				return parseDomain((String)get("url"));
			} else {
				return null;
			}
		}
	}
	
	public void setDomainName(String domainName) {
		put("domainName", domainName);
	}
	public int getStatus() {
		return get("status") == null ? 0 : (Integer)get("status");
	}
	public void setStatus(int status) {
		put("status", status);
	}
	public long getFetchTime() {		
		return get("fetchTime") == null ? 0L : (Long)get("fetchTime");
	}
	public void setFetchTime(long fetchTime) {
		put("fetchTime", fetchTime);
	}
	public int getFetchInterval() {
		return get("fetchInterval") == null ? 0 : (Integer)get("fetchInterval");
	}
	public void setFetchInterval(int fetchInterval) {
		put("fetchInterval", fetchInterval);
	}
	public long getModifiedTime() {
		return get("modifiedTime") == null ? 0L : (Long)get("modifiedTime");
	}
	public void setModifiedTime(long modifiedTime) {
		put("modifiedTime", modifiedTime);
	}
	public long getPrevFetchTime() {		
		return get("prevFetchTime") == null ? 0L : (Long)get("prevFetchTime");
	}
	public void setPrevFetchTime(long prevFetchTime) {
		put("prevFetchTime", prevFetchTime);
	}
	public long getPrevModifiedTime() {
		return get("prevModifiedTime") == null ? 0L : (Long)get("prevModifiedTime");
	}
	public void setPrevModifiedTime(long prevModifiedTime) {
		put("prevModifiedTime", prevModifiedTime);
	}
	
	public void setRawHtml(String html) {
//		try {
//			put("rawHtml", Snappy.compress(html));
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		put("rawHtml", html);
	}
    
    // html in compressed version
	public void setRawHtmlCompressed(byte[] html) {
		put("rawHtml", html);	
	}
	
	public String getRawHtml() {
//		try {
//			return new String(getRawHtmlUnCompressed(), "UTF-8");
//		} catch (UnsupportedEncodingException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//			return null;
//		}
		return get("rawHtml") == null ? null : (String)get("rawHtml");
	}
	
	public byte[] getRawHtmlCompressed() {
		return get("rawHtml") == null ? null : (byte[])get("rawHtml");
	}
	
	public byte[] getRawHtmlUnCompressed() {
		try {
			return get("rawHtml") == null ? null : Snappy.uncompress((byte[])get("rawHtml"));
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
			return null;
		}
	}
	
	public void setLinkMD5(String linkMD5) {
		put("linkMD5", linkMD5);
	}
	
	public String getLinkMD5() {
		return get("linkMD5") == null ? null : (String)get("linkMD5");
	}
	
	public void setContentLength(int len) {
		put("cLen", len);
	}
	
	public Integer getContentLength() {
		if(get("cLen") == null) {
			if(getRawHtml() != null) {
				int clen = URLDiscover.getContentLength(getRawHtml());
				setContentLength(clen);
				return clen;
			} else {
				return -1;
			}
		} else {
			return (Integer)get("cLen");
		}
	}
	
	public void setMainContent(String content) {
		put("content", content);
	}
	
	public String getMainContent() {
		return get("content") == null ? null : (String)get("content");
	}
	
	
	// for article
	
	public void setTitle(String title) {
		put("article_title", title);
	}
	
	public String getTitle() {
		if(containsKey("article_title")) {
			return (String) get("article_title");
		} else {
			return null;
		}
	}
    
    public void setAuthor(String author) {
		put("author", author);
	}
    
	public String getAuthor() {
		if(containsKey("author")) {
			return (String) get("author");
		} else {
			return null;
		}
	}
    
	/**
	 * Set Timestamp in seconds
	 * @param time
	 */
    public void setPublishedDate(long time) {
    	put("timestamp", time);
    }
    
    /**
     * 
     * @return timestamp in seconds
     */
    public long getPublishedDate() {
    	return get("timestamp") == null ? 0 : (Long)get("timestamp");
	}
	
	public void setDescription(String desc) {
    	put("desc", desc);
    }
    
    public String getDescription() {
		if(containsKey("desc")) {
			return (String) get("desc");
		} else {
			return null;
		}
	}
    
    public void setMetaDescription(String desc) {
    	put("meta_desc", desc);
    }
    
    public String getMetaDescription() {
		if(containsKey("meta_desc")) {
			return (String) get("meta_desc");
		} else {
			return null;
		}
	}
    
    public void setMetaKeywords(String kwds) {
    	put("meta_kwds", kwds);
    }
    
    public String getMetaKeywords() {
		if(containsKey("meta_kwds")) {
			return (String) get("meta_kwds");
		} else {
			return null;
		}
	}
    
    public void setScore(Integer score) {
    	put("score", score);
    }
    
    public Integer getScore() {
		if(containsKey("score")) {
			return (Integer) get("score");
		} else {
			return -1;
		}
	}
    
	public void setTaggedKeywords(List<String> tagged_kwds) {
		put("tagged_kwds", tagged_kwds);
	}
	
	@SuppressWarnings("unchecked")
	public List<String> getTaggedKeywords() {
		if(containsKey("tagged_kwds")) {
			return (List<String>) get("tagged_kwds");
		} else {
			return null;
		}
	}
	
    public static void main(String[] args) throws Exception {
    	WebPage test = new WebPage();
    	test.setDomainName("cnn.com");
    	test.setUrl("http://www.cnn.com");
    	test.setFetchInterval(2);
    	
    	System.out.println("MD5 key for " + test.getUrl() + " is:" + test.getUniqKey());
    	
    	WebPage test2 = new WebPage();
    	test2.setUrl("http://javarevisited.blogspot.com/2013/03/generate-md5-hash-in-java-string-byte-array-example-tutorial.html");
    	
    	System.out.println("The domain for " + test2.getUrl());
    	System.out.println(test2.getDomainName());
    	System.out.println("MD5 key is: " + test2.getUniqKey());
    	System.out.println("Cleaned url is :" + test2.getCleanedUrl());
    }


}
