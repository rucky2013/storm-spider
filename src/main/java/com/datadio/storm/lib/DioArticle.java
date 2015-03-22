package com.datadio.storm.lib;

import java.util.HashMap;

public class DioArticle extends HashMap<String, Object>{
	private static final long serialVersionUID = 1L;
	
	public void setTitle(String title) {
		put("title", title);
	}
	
	public String getTitle() {
		if(containsKey("title")) {
			return (String) get("title");
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
    
    public void setPublishedDate(long time) {
    	put("timestamp", time);
    }
    
    public long getPublishedDate() {
    	return get("timestamp") == null ? 0 : (Long)get("timestamp");
	}
    
    // finalUrl
    public void setLink(String url) {
    	put("link", url);
    }
    
	public String getLink() {
		if(containsKey("link")) {
			return (String) get("link");
		} else {
			return null;
		}
	}
    
    public void setContent(String content) {
    	put("content", content);
    }
    
    public String getContent() {
		if(containsKey("content")) {
			return (String) get("content");
		} else {
			return null;
		}
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
    
    public long getPrevFetchTime() {		
		return get("prevFetchTime") == null ? 0L : (Long)get("prevFetchTime");
	}
	public void setPrevFetchTime(long prevFetchTime) {
		put("prevFetchTime", prevFetchTime);
	}
	
	public int getFetchInterval() {
		return get("fetchInterval") == null ? 0 : (Integer)get("fetchInterval");
	}
	public void setFetchInterval(int fetchInterval) {
		put("fetchInterval", fetchInterval);
	}
}
