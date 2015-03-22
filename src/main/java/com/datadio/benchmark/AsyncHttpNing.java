package com.datadio.benchmark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutionException;
//import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.AsyncHttpClientConfig.Builder;
import com.ning.http.client.Request;
import com.ning.http.client.RequestBuilder;
import com.ning.http.client.Response;


public class AsyncHttpNing {
	
	AsyncHttpClient asyncHttpClient;
	
	private static BlockingDeque<Future<Response>> fetchQueue;
	
	public AsyncHttpNing() {
		Builder builder = new AsyncHttpClientConfig.Builder();
		builder.setCompressionEnabled(true)
		.setAllowPoolingConnection(true)
	    .setConnectionTimeoutInMs(3000)
	    .setUserAgent("Mozilla/5.0 (Windows; U; Windows NT 6.1; ja; rv:1.9.2a1pre) Gecko/20090403 Firefox/3.6a1pre")
	    .build();
		asyncHttpClient = new AsyncHttpClient(builder.build());
	}
	
	// sync url fetch
	public byte[] fetchUrl(String url) {
		try {
			Future<Response> f = asyncHttpClient.prepareGet(url).execute();
			Response response = f.get();
			return response.getResponseBodyAsBytes();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public void enqueueUrl(String url) {
		try {
			Future<Response> f = asyncHttpClient.prepareGet(url).execute();
			fetchQueue.add(f);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	
	public void batchGet() {
		while(fetchQueue.size() > 0) {
			Future<Response> f = fetchQueue.removeFirst();
			try {
				Response response = f.get();
				try {
					System.out.println(">>>>>>>>> finished url request: " + response.getUri());
				} catch (MalformedURLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	// async url fetch
	public void aFetchUrl(String url) {
		System.out.println(">>>>>>>>> Fetching url:" + url);
		
		RequestBuilder requestBuilder = new RequestBuilder();
		Request thisRequest = requestBuilder.setUrl(url)
				.addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
				.addHeader("ACCEPT_LANGUAGE", "en-US,en;q=0.8")
				.build();
		
    	try {
//    		Future<Response> re = asyncHttpClient.prepareGet(url).execute(new AsyncCompletionHandler<Response>(){
    		asyncHttpClient.executeRequest(thisRequest, new AsyncCompletionHandler<Response>(){
				public Response onCompleted(Response response) throws Exception {
//					byte[] content = response.getResponseBodyAsBytes();
//					Thread.sleep(3000);
			    	System.out.println(">>>>>>>>> finished url request: " + response.getUri());
//            	latch.countDown();
			    	return response;
			    }
			    
				public void onThrowable(Throwable t){
			        // Something wrong happened.
			    }
			});			
//    		try {
//				Response resp = re.get();
//				System.out.println("Get the response" + resp.toString());
//			} catch (InterruptedException | ExecutionException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
    		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	public static void main(final String[] args) throws Exception {
		AsyncHttpNing h = new AsyncHttpNing();
		
		String[] urls = {
//				"http://www.cnn.com",
//				"http://www.techcrunch.com/",
//				"http://www.huffingtonpost.com",
//				"http://www.washingtonpost.com/",
//				"http://www.nytimes.com"
				"http://dev.thelinkbuilders.com/checkme.php"
		};
		
		for(String url : urls) {
			
			h.aFetchUrl(url);
		}
        System.out.println("Done");
        
//		String filePath = "./data/rob_init_urls.txt";
//        BufferedReader br = new BufferedReader(new FileReader(filePath));
//        
//        try {
//	        String url = br.readLine();
//	        long start = System.currentTimeMillis();
//	        while (url != null) {
//	        	System.out.println("Reading url: " + url);
//	        	h.aFetchUrl(url);
////	        	h.enqueueUrl(url);
//	        	url = br.readLine();
//	        }
//	        long end = System.currentTimeMillis();
//	        long spent = end - start;
//	        System.out.println("Spent: " + spent);
//	    } finally {
//	        br.close();
//	    }
        
//        h.batchGet();
    }
}

