package com.datadio.storm.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datadio.storm.lib.MD5Signature;
import com.datadio.storm.lib.WebPage;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

public class PageRedis {
	
	public static final String DOMAIN_QUEUE = "crawler:domain";
	public static final String DOMAIN_SET = "parsed:domain"; // holds domains that is parsed before
	public static final String QUEUE_NAME_PREFIX = "domain:";
	public static final String URL_NAME_PREFIX = "url:";
	
	public static final String RSS_NAME_PREFIX = "rss:";
	public static final String RSS_QUEUE = "crawler:rss";
	
	public static final String RSS_LAST_REQUEUE = "crawler:requeue:rss";
	public static final String URL_LAST_REQUEUE = "crawler:requeue:url";
	
	public static final int URL = 0;
    public static final int RSS = 1;
    
    public static final int DEFAULT_PORT = 6379;
	
	private final JedisPool redis_pool;
	private static final Logger LOG = LoggerFactory.getLogger(PageRedis.class);
	
	public PageRedis() {
		this("localhost", DEFAULT_PORT, 10000);
	}
	
	public PageRedis(String url) {
		// default timeout is 2 seconds.
		this(url, DEFAULT_PORT, 10000);
	}
	
	public PageRedis(String host, int port, int timeout) {
		JedisPoolConfig redis_conf = new JedisPoolConfig();
		redis_conf.setMaxActive(5);
		redis_pool = new JedisPool(redis_conf, host, port, timeout);
	}
	
//	public void addToQueue(String... keys) {
//		Jedis jedis_conn = redis_pool.getResource();
//		int expire_time = 15;
//		long curTime = System.currentTimeMillis();
//		try {
//			for(String key : keys) {
//				if(!jedis_conn.exists(key)) {
//					jedis_conn.rpush(QUEUE_NAME, key);
//					jedis_conn.set(key, Long.toString(curTime));
//					jedis_conn.expire(key, expire_time);
//				}
//			}
//			
//		} finally {
//			redis_pool.returnResource(jedis_conn);
//		}
//	}
	
	public List<String> getRangeQueue(String key, long num) {
		Jedis jedis_conn = redis_pool.getResource();
		List<String> pageKeys = null;
		try {
			pageKeys = jedis_conn.lrange(key, 0, num-1);
			jedis_conn.ltrim(key, num, -1);
			
		} finally {
			redis_pool.returnResource(jedis_conn);
		}
		
		return pageKeys;
	}
	
	public long getQueueLength(String queueName) {
		Jedis jedis_conn = redis_pool.getResource();
		long len;
		
		try {
			len = jedis_conn.llen(queueName);
			
		} finally {
			redis_pool.returnResource(jedis_conn);
		}
		
		return len;
	}
	
	/**
	 * Add url to redis queue to be crawled
	 * @param page
	 * @param expire_time
	 */
	public void addToQueue(WebPage page, int expire_time) {
		long curTime = System.currentTimeMillis();	
		insertToURLQueue(MD5Signature.getMD5(page.getDomainName()), page.getUniqKey(), curTime, expire_time);
	}
	
	public void addToQueue(Map<String, String> keys, int expire_time) {
		long curTime = System.currentTimeMillis();
		for(Map.Entry<String, String> entry : keys.entrySet()) {
			insertToURLQueue(entry.getValue(), entry.getKey(), curTime, expire_time);
		}
	}
	
	private void insertToURLQueue(String domainKey, String urlKey, long inQueueTime, int expireTime) {
		
		String pageKeyRedis = URL_NAME_PREFIX + urlKey;
		String queueName =  QUEUE_NAME_PREFIX + domainKey;
		
		Jedis jedis_conn = redis_pool.getResource();
		try {
			// add new url to domain queue
//			System.out.println(">>> key value:  " + pageKeyRedis);
			if(!jedis_conn.exists(pageKeyRedis)) {
				jedis_conn.rpush(queueName, urlKey);
				jedis_conn.set(pageKeyRedis, Long.toString(inQueueTime));
				jedis_conn.expire(pageKeyRedis, expireTime);
				LOG.debug(">>> Added url: " + urlKey + " to queue: " + domainKey);
			}
			
			// new domain, push to fetch queue
			if(!jedis_conn.sismember(DOMAIN_SET, queueName)) {
				jedis_conn.sadd(DOMAIN_SET, queueName);
//				System.out.println(">>> Put into Domain Queue:" + queueName);
				jedis_conn.rpush(DOMAIN_QUEUE, queueName);
			}
			
//		} catch (Exception e) {
//			// TODO: handle exception
//			e.printStackTrace();
		} finally {
			redis_pool.returnResource(jedis_conn);
		}
	}
	
	public void addToDomainQueue(List<String> domainNames) {
		int keySize;
		keySize = domainNames.size();
		if(keySize <= 0) return;
		
		String[] domainKeys = new String[keySize];
		for (int i = 0; i < keySize; i++) {
			domainKeys[i] = QUEUE_NAME_PREFIX + MD5Signature.getMD5(domainNames.get(i));
			LOG.debug(">>>>>>> Add domain to queue: " + domainNames.get(i));
		}

		Jedis jedis_conn = redis_pool.getResource();
		
		try {    		
    		jedis_conn.rpush(DOMAIN_QUEUE, domainKeys);
		} finally {
			redis_pool.returnResource(jedis_conn);
//			redis_pool.returnBrokenResource(jedis_conn);
		}
	}
	
	// pipliend version of rpush
	public void addToDomainQueuePiplined(List<String> domainNames) {
		
//		System.out.println(">>>>>>> About to add domain in queue");
		if(domainNames.size() <= 0) return;

		Jedis jedis_conn = redis_pool.getResource();
		
		try {
			Pipeline redis_pip = jedis_conn.pipelined();
//			System.out.println(">>>>>>> Going to add these to the queue: ");
			for(String domainName : domainNames) {
				String md5Key = QUEUE_NAME_PREFIX + MD5Signature.getMD5(domainName);
//				System.out.println(">>>>>>> Add domain to queue: " + domainName);
				LOG.debug(">>>>>>> Add domain to queue: " + domainName);
				redis_pip.rpush(DOMAIN_QUEUE, md5Key);
			}
			redis_pip.sync();
		} finally {
			redis_pool.returnResource(jedis_conn);
		}
	}
	
	public List<String> popDomain(long num) {
		Jedis jedis_conn = redis_pool.getResource();
		List<String> urls = new ArrayList<String>();
		try {
			String domainName = jedis_conn.lpop(DOMAIN_QUEUE);
			if(domainName!=null){
				urls = jedis_conn.lrange(domainName,0,num-1);
				jedis_conn.ltrim(domainName, num, -1);
				jedis_conn.srem(DOMAIN_SET, domainName);
			}
			
		} finally {
			redis_pool.returnResource(jedis_conn);
		}
		
		return urls;
	}
	
	public List<String> getUrlsFromDomains(long numDom, long numUrl) {
		Jedis jedis_conn = redis_pool.getResource();
		List<String> allUrls = new ArrayList<String>();
		try {
			List<String> domains = jedis_conn.lrange(DOMAIN_QUEUE, 0, numDom-1);
			if(domains != null && !domains.isEmpty()) {
				for(String domain : domains) {
					jedis_conn.srem(DOMAIN_SET, domain);
					List<String> urls = jedis_conn.lrange(domain, 0, numUrl-1);
					if(urls != null && !urls.isEmpty()) {
						allUrls.addAll(urls);
						jedis_conn.ltrim(domain, numUrl, -1);
					}
				}
				
			}
			jedis_conn.ltrim(DOMAIN_QUEUE, numDom, -1);
			
		} finally {
			redis_pool.returnResource(jedis_conn);
		}
		return allUrls;
	}
	
	public void addToRSSQueue(WebPage page, int expire_time) {
		// did not use the getUniqKey method of WebPage class. Since that one is used on cleaned key.
//		String pageKey = MD5Signature.getMD5(page.getUrl());
//		String pageKeyRedis = RSS_NAME_PREFIX + pageKey;
		String pageKey = page.getUniqKey();
		Jedis jedis_conn = redis_pool.getResource();
		
//		long curTime = System.currentTimeMillis();
		try {
			jedis_conn.rpush(RSS_QUEUE, pageKey);
			
		} finally {
			redis_pool.returnResource(jedis_conn);
		}
	}
	
	public void addToRSSQueue(Map<String, String> keys) {
		
		Jedis jedis_conn = redis_pool.getResource();
		
		try {
			Pipeline redis_pip = jedis_conn.pipelined();
			for(Map.Entry<String, String> entry : keys.entrySet()) {
				redis_pip.rpush(RSS_QUEUE, entry.getKey());
			}
			redis_pip.sync();
		} finally {
			redis_pool.returnResource(jedis_conn);
		}
	}
	
	public List<String> getFeeds(long numFeed) {
		Jedis jedis_conn = redis_pool.getResource();
		List<String> feeds;
		try {
			feeds = jedis_conn.lrange(RSS_QUEUE, 0, numFeed-1);
			if(feeds != null && !feeds.isEmpty()) {
				jedis_conn.ltrim(RSS_QUEUE, numFeed, -1);
			}
			
		} finally {
			redis_pool.returnResource(jedis_conn);
		}
		return feeds;
	}
	
	// WARN: it will wipe ALL the data!!
	public void cleanDomainKeys() {
		Jedis jedis_conn = redis_pool.getResource();
		Set<String>dk = jedis_conn.keys(QUEUE_NAME_PREFIX + "*");
		
		try {
			for(String urlDomain:dk) {
				List<String> urls = jedis_conn.lrange(urlDomain,0,-1);
				for(String url:urls) {
					jedis_conn.del(URL_NAME_PREFIX + url);
				}
				jedis_conn.del(urlDomain);
			}
			jedis_conn.del(DOMAIN_QUEUE);
			jedis_conn.del(DOMAIN_SET);
		} finally {
			redis_pool.returnResource(jedis_conn);
		}
	}
	
	
	public String getNextQueueTime(String queueName) {
		String timeKey = null;
		Jedis jedis_conn = redis_pool.getResource();
		try {
			timeKey = jedis_conn.get(queueName);
		} finally {
			redis_pool.returnResource(jedis_conn);
		}
		
		return timeKey;
	}
	
	public void setNextQueueTime(String queueName, String timeKey) {
		Jedis jedis_conn = redis_pool.getResource();
		try {
			jedis_conn.set(queueName, timeKey);
		} finally {
			redis_pool.returnResource(jedis_conn);
		}
	}
	
	// enqueue all domain keys into domain queue
	public void enqueueAllDomainKeys() {
		Jedis jedis_conn = redis_pool.getResource();
		Set<String>dk = jedis_conn.keys(QUEUE_NAME_PREFIX + "*");
		try {
			for(String urlDomain:dk) {
				jedis_conn.rpush(DOMAIN_QUEUE, urlDomain);
			}
		} finally {
			redis_pool.returnResource(jedis_conn);
		}
	}
	
	public static void main(String[] args) throws Exception {
		PageRedis redis_conn = new PageRedis();
//		redis_conn.enqueueAllDomainKeys();
//		System.out.println("Successfully re-euqueued all the domain urls");
//		redis_conn.cleanDomainKeys();
//		System.out.println("Successfully cleaned the entire keys for urls");
		List<String> urls = redis_conn.getUrlsFromDomains(5, 100);
		for(String url : urls) {
			System.out.println("Get url: " + url);
		}
	}
}
