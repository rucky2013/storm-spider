package com.datadio.storm.fetcher;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

//import com.datadio.storm.lib.DioConfig;
import com.datadio.storm.lib.WebPage;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.Request;
import com.ning.http.client.RequestBuilder;
import com.ning.http.client.Response;
import com.ning.http.client.AsyncHttpClientConfig.Builder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class URLAsyncFetchBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 1L;
	
	OutputCollector _collector;
	URLDiscover fetcher;
	
	private static final Logger LOG = LoggerFactory.getLogger(URLAsyncFetchBolt.class);

//    private AtomicInteger activeThreads = new AtomicInteger(0);
//    private AtomicInteger spinWaiting = new AtomicInteger(0);
    private BlockingDeque<WebPage> fetchQueue;

    @SuppressWarnings("rawtypes")
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    	_collector = collector;
		fetcher = new URLDiscover();
		fetchQueue = new LinkedBlockingDeque<WebPage>();
		
		int threadCount = 5;
		long maxCrawlDelay = 5000L;
		
		for (int i = 0; i < threadCount; i++) { // spawn threads
            new FetcherThread(maxCrawlDelay).start();
        }
    }

    /**
     * This class picks items from queue and fetches the pages.
     */
    private class FetcherThread extends Thread {

        private long maxCrawlDelay;
        private AsyncHttpClient asyncHttpClient;

        public FetcherThread(long maxCrawlDelay) {
            this.setDaemon(true); // don't hang JVM on exit
            this.setName("UrlFetcherThread"); // use an informative name
//            this.maxCrawlDelay = conf.getInt("fetcher.max.crawl.delay", 30) * 1000;
            this.maxCrawlDelay = maxCrawlDelay;
            Builder builder = new AsyncHttpClientConfig.Builder();
    		builder.setCompressionEnabled(true)
//    		.setAllowPoolingConnection(true)
    	    .setConnectionTimeoutInMs(3000)
    	    .setUserAgent("Mozilla/5.0 (Windows; U; Windows NT 6.1; ja; rv:1.9.2a1pre) Gecko/20090403 Firefox/3.6a1pre")
    	    .build();
    		
    		asyncHttpClient = new AsyncHttpClient(builder.build());
        }

        public void run() {
    		WebPage page = null;
    		while (true) {
    			if(fetchQueue.size() > 0) {
    				try {
    					page = fetchQueue.removeFirst();
					} catch (Exception e) {
						try { Thread.sleep(100); } catch (InterruptedException e1) {}
						continue;
					}
    				
//    				spinWaiting.incrementAndGet();
    			} else {
    				try { Thread.sleep(100); } catch (Exception ex) {}
    				continue;
    			}
  
                String url = page.getCleanedUrl();
//                activeThreads.incrementAndGet(); // count threads
//                LOG.info(getName() + " => activeThreads=" + activeThreads
//                    + ", spinWaiting=" + spinWaiting);
                
                final WebPage newPage = page;
                
                RequestBuilder requestBuilder = new RequestBuilder();
        		Request thisRequest = requestBuilder.setUrl(url)
        				.addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
        				.addHeader("ACCEPT_LANGUAGE", "en-US,en;q=0.8")
        				.build();
        		
                try {
					asyncHttpClient.executeRequest(thisRequest, new AsyncCompletionHandler<Response>(){
						public Response onCompleted(Response response) throws Exception {
//							int status = response.getStatusCode();
//							System.out.println(" >>>>>>>>>>> The return status for " + response.getUri() + " : " + status);
							byte[] contentBytes = response.getResponseBodyAsBytes();
//							byte[] contentBytes = Snappy.compress(response.getResponseBodyAsBytes());
//							_collector.emit(new Values(newPage, contentBytes, status));
							if(contentBytes != null && contentBytes.length > 0) {
								synchronized (_collector) {
									_collector.emit("LinkDiscover", new Values(Collections.unmodifiableMap(newPage), contentBytes));
									_collector.emit("ContentParser", new Values(Collections.unmodifiableMap(newPage), contentBytes));
								}
								
							}
//							System.out.println("Emiting url");
							return response;
					    }
					    
						public void onThrowable(Throwable t){
					        // Something wrong happened.
					    }
					});
				} catch (IOException e) {
					LOG.error(e.getMessage(), e);
				}
    		}
        }
    }
    
    @SuppressWarnings("unchecked")
	public void execute(Tuple tuple) {
//    	WebPage page = (WebPage)tuple.getValue(0);
    	WebPage page = new WebPage((Map<String, Object>)tuple.getValue(0));
    	fetchQueue.add(page);
        _collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//    	declarer.declare(new Fields("origin", "webpages", "status"));
//    	declarer.declare(new Fields("origin", "webpages"));
    	declarer.declareStream("LinkDiscover", new Fields("originPage", "rawContent"));
    	declarer.declareStream("ContentParser", new Fields("originPage", "rawContent"));
    }
	
}
