package com.datadio.storm.scheduler;

import com.datadio.storm.lib.WebPage;

public class FetchScheduler {
	
	private float INC_RATE;
	private float DEC_RATE;
	private int MAX_INTERVAL;
	private int MIN_INTERVAL;
	private boolean SYNC_DELTA;
	private double SYNC_DELTA_RATE;
	
	/** It is unknown whether page was changed since our last visit. */
	public static final int STATUS_UNKNOWN       = 0;
	/** Page is known to have been modified since our last visit. */
	public static final int STATUS_MODIFIED      = 1;
	/** Page is known to remain unmodified since our last visit. */
	public static final int STATUS_NOTMODIFIED   = 2;
	
	public static final int SECONDS_PER_DAY = 86400;
	
	protected int defaultInterval;
	
	public FetchScheduler() {

		INC_RATE = 0.2f;
		DEC_RATE = 0.2f;
		MIN_INTERVAL = 1;
		MAX_INTERVAL = SECONDS_PER_DAY;
		SYNC_DELTA = true;
		SYNC_DELTA_RATE = 0.2f;
		
		defaultInterval = 0;
	}
	
	public void initializeScheduler(WebPage page) {
	    page.setFetchTime(System.currentTimeMillis());
	    page.setFetchInterval(defaultInterval);
	}
	
	public void updateFetchScheduler(WebPage page) {
		setFetchScheduler(page, page.getPrevFetchTime(), 
				page.getPrevModifiedTime(), page.getFetchTime(), 
				page.getModifiedTime(), page.getStatus());
	}
	
	public void setFetchScheduler(
			 	WebPage page,
			 	long prevFetchTime, long prevModifiedTime,
			 	long fetchTime, long modifiedTime, int state) {
		
		long curTime = System.currentTimeMillis();
	    // overwrite init data or if it's too old
	    if (fetchTime <= 0 || (fetchTime + MAX_INTERVAL * 1000L * 2 < curTime)) {
	    	fetchTime = curTime;
	    }
	    
	    long refTime = fetchTime;
	    if (modifiedTime <= 0) modifiedTime = fetchTime;
	    int interval = page.getFetchInterval();
	    
	    switch (state) {
	      case FetchScheduler.STATUS_MODIFIED:
	        interval *= (1.0f - DEC_RATE);
	        break;
	      case FetchScheduler.STATUS_NOTMODIFIED:
	        interval *= (1.0f + INC_RATE);
	        break;
	      case FetchScheduler.STATUS_UNKNOWN:
	        break;
	    }
	    page.setFetchInterval(interval);
	    if (SYNC_DELTA) {
	      int delta = (int) ((fetchTime - modifiedTime) / 1000L) ;
	      if (delta > interval) interval = delta;
	      refTime = fetchTime - Math.round(delta * SYNC_DELTA_RATE);
	    }
	    if (interval < MIN_INTERVAL) interval = MIN_INTERVAL;
	    if (interval > MAX_INTERVAL) interval = MAX_INTERVAL;
	    
	    page.setFetchTime(refTime + interval * 1000L);
	    page.setPrevFetchTime(fetchTime);
	    page.setModifiedTime(modifiedTime);
	    page.setPrevModifiedTime(prevModifiedTime);
	}
	
	public long calculateLastFetchTime(WebPage page) {
	    return page.getFetchTime() - page.getFetchInterval() * 1000L;
	}
	
	public boolean shouldFetch(WebPage page, long curTime) {
		// pages are never truly GONE - we have to check them from time to time.
	    // pages with too long fetchInterval are adjusted so that they fit within
	    // maximum fetchInterval (batch retention period).
	    long fetchTime = page.getFetchTime();
	    if (fetchTime - curTime > MAX_INTERVAL * 1000L) {
	      if (page.getFetchInterval() > MAX_INTERVAL) {
	        page.setFetchInterval(Math.round(MAX_INTERVAL * 0.9f));
	      }
	      page.setFetchTime(curTime);
	    }
	    return fetchTime <= curTime;
	}
}
