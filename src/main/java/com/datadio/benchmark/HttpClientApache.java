package com.datadio.benchmark;
//package com.datadio.storm.lib;
//
//import java.io.BufferedReader;
//import java.io.FileReader;
//
//import org.apache.http.HttpResponse;
//import org.apache.http.client.HttpClient;
//import org.apache.http.client.methods.HttpGet;
//import org.apache.http.impl.client.DefaultHttpClient;
//import org.apache.http.impl.conn.PoolingClientConnectionManager;
//import org.apache.http.params.HttpParams;
//import org.apache.http.protocol.BasicHttpContext;
//import org.apache.http.protocol.HttpContext;
//
//public class HttpClientApache extends Thread{
//	
//	private final HttpClient httpClient;
//    private final HttpContext context;
//    private final HttpGet httpget;
//    private final String url;
//    
//	public HttpClientApache(HttpClient httpClient, HttpGet httpget, String url) {
//		this.httpClient = httpClient;
//        this.context = new BasicHttpContext();
//        this.httpget = httpget;
//        this.url = url;
//	}
//	
//	public void run() {
//		System.out.println("Fetch url: " + url);
//
//        try {
//            // execute the method
//            HttpResponse response = httpClient.execute(httpget, context);
//
//        } catch (Exception e) {
//            httpget.abort();
//            System.out.println(url + " - error: " + e);
//        }
//	}
//	
//	public static void main(String[] args) throws Exception {
//		String filePath = "./data/rob_init_urls.txt";
//        BufferedReader br = new BufferedReader(new FileReader(filePath));
//        PoolingClientConnectionManager cm = new PoolingClientConnectionManager();
//        cm.setMaxTotal(1000);
//        HttpClient httpclient = new DefaultHttpClient(cm);
//        HttpParams params = httpClient.getParams();
//        try {
//	        String url = br.readLine();
//	        long start = System.currentTimeMillis();
//	        while (url != null) {
//	        	System.out.println("Reading url: " + url);
//	        	HttpGet httpget = new HttpGet(url);
//	        	HttpClientApache fetchThread = new HttpClientApache(httpclient, httpget, url);
//	        	fetchThread.start();
//	        	url = br.readLine();
//	        }
//	        long end = System.currentTimeMillis();
//	        long spent = end - start;
//	        System.out.println("Spent: " + spent);
//	    } finally {
//	        br.close();
//	        httpclient.getConnectionManager().shutdown();
//	    }
//	}
//
//}
