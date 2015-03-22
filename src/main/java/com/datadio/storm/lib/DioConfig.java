package com.datadio.storm.lib;

import java.util.HashMap;

public class DioConfig extends HashMap<String, Object>{
	
	private static final long serialVersionUID = 1L;

	public void setRedisHost(String url) {
		put("redis", url);
	}
	
	public String getRedisHost() {
		if(containsKey("redis")) {
			return (String) get("redis");
		} else {
			return "localhost";
		}
		
	}
	
	public void setCassandraHost(String urls) {
		put("cassandra", urls);
	}
	
	public String getCassandraHost() {
		if(containsKey("cassandra")) {
			return (String) get("cassandra");
		} else {
			return "127.0.0.1:9160";
		}
	}
	
	public void setSqlHost(String url, String database, String user, String password) {
		put("sql:url", url);
		put("sql:database", database);
		put("sql:username", user);
		put("sql:password", password);
	}
	
	public void setUrlKeyExpireSec(int expire_sec) {
		put("UrlKeyExpireSec", expire_sec);
	}
	
	public int getUrlKeyExpireSec() {
		if(containsKey("UrlKeyExpireSec")) {
			return (Integer) get("UrlKeyExpireSec");
		} else {
			return 0;
		}
	}
}
