package com.datadio.storm.parser;

import com.gravity.goose.Article;
import com.gravity.goose.Configuration;
import com.gravity.goose.Goose;

import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.extractors.ArticleExtractor;
import static org.apache.commons.lang.StringEscapeUtils.unescapeHtml;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.datadio.storm.fetcher.DioHttpClient;
import com.datadio.storm.fetcher.URLDiscover;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContentParser {
	
	Goose goose;
	DioHttpClient dioHttp;
	
	private static final Logger LOG = LoggerFactory.getLogger(ContentParser.class);
	
	public ContentParser() {
		Configuration configuration = new Configuration();
        configuration.setEnableImageFetching(false);
        goose = new Goose(configuration);
        dioHttp = new DioHttpClient();
	}
	
	public byte[] getDoc(String url) {
		try {
			// We need to utilize the DioHttpClient to get past all of the anti-bot & problems. 
			return dioHttp.exec(url, "DioFeed");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	
	public String extractByBoilerpipe(String doc) {
		String content = null;
		try {
			content = unescapeHtml(ArticleExtractor.INSTANCE.getText(doc));
		} catch (BoilerpipeProcessingException e) {
			e.printStackTrace();
		}
		
		return content;
	}
	
	public String extractContent(String url) {
		byte[] rawHtml = getDoc(url);
		return extractContent(url, new String(rawHtml));
	}
	
	public String extractContent(String url, String rawHtml) {
		// have to use url, otherwise not working
		String content = null;
		
		try {
			Article article = goose.extractContent(url, rawHtml);
			content = unescapeHtml(article.cleanedArticleText());
		} catch (NullPointerException e) {
			
//			System.out.println("<<<< Failed to extract content from : " + url);
//			System.out.println(rawHtml);
//			System.out.println();
			return null;
			
		} catch (ArrayIndexOutOfBoundsException e) {
			System.out.println("<<<< Failed to use Groose to extract content from : " + url);
			System.out.println(rawHtml);
			System.out.println();
			
			LOG.error("Failed to use Groose to extract content from : " + url);
		}
		
//        System.out.println(article.publishDate());
//        System.out.println(article.canonicalLink());
//        System.out.println(article.metaDescription());
//        System.out.println(article.metaKeywords());
        
        
         
        if(content == null || content.isEmpty()) {
        	try {
        		content = extractByBoilerpipe(rawHtml);
			} catch (ArrayIndexOutOfBoundsException e) {
				System.out.println("<<<< Failed to use Bilerpipe to extract content from : " + url);
				LOG.error("Failed to use Bilerpipe to extract content from : " + url);
			}
             
//             System.out.println("=== Content from Boiler: ");             
//             System.out.println(content);
        }
        
        return content;
	}
	
	public void init(String profileDirectory) throws LangDetectException {
		System.out.println("Loaded all Language Detection Libs ...");
		
		if(profileDirectory == null) {
			profileDirectory = System.getProperty("user.dir") + "/" + "data/profiles";
		}
		
        DetectorFactory.loadProfile(profileDirectory);
    }
	
	public String detectLanguage(String content) {
        try {
			Detector detector = DetectorFactory.create();
	        detector.append(content);
	        return detector.detect();
	        
		} catch (LangDetectException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
    
	public static void main(String[] args) {
    	
//    	String url = "http://www.micrositemasters.com/blog/the-top-101-overlooked-or-overemphasized-factors-in-seo/";
//    	String url = "http://www.tanteifile.com/baka/2007/09/03_01/";
    	String url = "https://news.google.com/";
    	
    	URLDiscover test = new URLDiscover();
    	byte[] contentBytes = null;
    	
    	contentBytes = test.fetchDocumment(url);
    	
    	ContentParser a = new ContentParser();
    	a.extractContent(url,new String(contentBytes));
    	
    	Map<String,Object> foundLinks = test.findLinks(url, new String(contentBytes));
    	List<String> links = (List<String>) foundLinks.get("url");
    	
    	System.out.println("Found urls:");
    	for(String link : links) {
    		System.out.println();
    		System.out.println();
    		System.out.println("=====");
    		System.out.println("Starting fetching: " + link);
    		ContentParser f = new ContentParser();
            String content = f.extractContent(link);
//            System.out.println("lang: " + f.detectLanguage(content));
    	}
    	
        
                  
    }
}
