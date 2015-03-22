package com.datadio.benchmark;

import com.datadio.storm.fetcher.URLDiscover;
import com.datadio.storm.lib.MD5Signature;


public class UrlMD5 {
	public static void main(String[] args) throws Exception {
		String url = "http://techcrunch.com";
		URLDiscover urlFetch = new URLDiscover();
		byte[] urlDoc = urlFetch.fetchDocumment(url);
		String docString = new String(urlDoc);
		
		String text = URLDiscover.getCleanedHtml(docString);
		
		String linksStr = URLDiscover.getLinkMD5(docString);
		
		int len = URLDiscover.getContentLength(docString);
		
		String mdStr = MD5Signature.getMD5(urlDoc);
//		String mdStr2 = MD5Signature.getMD5(docString);
		String mdurl = MD5Signature.getMD5(url);
		String mdtext = MD5Signature.getMD5(text);
		
		System.out.println(linksStr);
		
		System.out.println(len);
		
		System.out.println("MD5 String for url: " + url);
		System.out.println(mdStr);
//		System.out.println(mdStr2);
		System.out.println(mdurl);
		System.out.println(mdtext);
	}
}
