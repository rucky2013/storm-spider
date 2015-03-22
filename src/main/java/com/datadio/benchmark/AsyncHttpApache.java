package com.datadio.benchmark;
//package com.datadio.storm.lib;
//
//import java.io.ByteArrayOutputStream;
//import java.io.IOException;
//import java.io.InputStream;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.Future;
//
//import org.apache.http.HttpResponse;
//import org.apache.http.client.config.RequestConfig;
//import org.apache.http.client.methods.HttpGet;
//import org.apache.http.concurrent.FutureCallback;
//import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
//import org.apache.http.impl.nio.client.HttpAsyncClients;
//
//public class AsyncHttpApache {
//	
//	private CloseableHttpAsyncClient httpclient;
////	private final CountDownLatch latch;
//	
//	public AsyncHttpApache() {
////		latch = new CountDownLatch(3);
//		
//		httpclient = HttpAsyncClients.createDefault();
//		RequestConfig requestConfig = RequestConfig.custom()
//        		.setSocketTimeout(10000)
//        		.setConnectTimeout(10000).build();
//		httpclient = HttpAsyncClients.custom()
//        		.setDefaultRequestConfig(requestConfig).build();
//	}
//	
//	public void fetchUrl(String url) {
//		httpclient.start();
//        try {
//        	System.out.println(">>>>>>>>> Fetching url:" + url);
//        	HttpGet request = new HttpGet(url);
////        	HttpResponse response = httpclient.execute(request, null).get();
//        	
//        	Future<HttpResponse> future = httpclient.execute(request, new FutureCallback<HttpResponse>() {
//                public void completed(final HttpResponse response) {
//                	System.out.println(response.getStatusLine());
//                	InputStream is = null;
//                	try {
//						is = response.getEntity().getContent();
//	                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
//	                    byte[] buffer = new byte[1024];
//	                    int nbRead;
//	                    while ((nbRead = is.read(buffer)) != -1) {
//	                        bos.write(buffer, 0, nbRead);
//	                    }
//	                    is.close();
//                	} catch (IllegalStateException | IOException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//                	System.out.println(">>>>>>>>> finished url request");
////                	latch.countDown();
//                }
//                
//                public void failed(final Exception ex) {
////                	latch.countDown();
//                	System.out.println(">>>>>>>>> failed url request" + ex);
//                }
//
//                public void cancelled() {
////                	latch.countDown();
//                	System.out.println(">>>>>>>>> cancelled url request");
//                }
//        	});
//        	
////        	 latch.await();
//        	while (!future.isDone()) {
//        		System.out.println("waiting..");
//        		Thread.sleep(1000);
//        	}
////        	future.get();
//            
//        } catch (Exception e) {
//           System.out.println("Exception while fetching " + url);
//           e.printStackTrace();
//       
//        } finally {
//        	try {
//				httpclient.close();
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//        }
//       
//	}
//	
//	public static void main(final String[] args) throws Exception {
//		
//		String[] urls = {
//				"http://www.cnn.com",
//				"http://www.techcrunch.com/",
//				"http://www.huffingtonpost.com",
//				"http://www.washingtonpost.com/",
//				"http://www.nytimes.com"
//		};
//	
//		
//		for(String url : urls) {
//			AsyncHttpApache h = new AsyncHttpApache();
//			h.fetchUrl(url);
//		}
//        System.out.println("Done");
//    }
//}
