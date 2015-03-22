package com.datadio.storm.fetcher;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.CookieHandler;
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Pattern;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.http.client.methods.HttpGet.*;
import org.apache.http.client.utils.URIUtils.*;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.ClientContext;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.params.ConnRouteParams;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.X509HostnameVerifier;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.DecompressingHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.safety.Whitelist;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datadio.storm.lib.MD5Signature;
import com.datadio.storm.lib.WebPage;

public class URLDiscover {
	private CookieStore cookieStore;
    private HttpContext localContext;
    private HashSet<String> visitedLinks;
    
    private final Pattern FILTERS = Pattern.compile(".*(\\.(css|js|bmp|gif|jpe?g" 
            + "|png|tiff?|mid|mp2|mp3|mp4|xls|xlsx|csv"
            + "|wav|avi|mov|mpeg|ram|m4v|pdf" 
            + "|rm|smil|wmv|swf|wma|zip|rar|gz|doc|pls|xml|exe|zip))$");

	private final Pattern TWITTER  = Pattern.compile("http(s)?://(www.)?twitter.com/(?!signup)(?!share)(?!home)(?!intent)(#!/)?([a-zA-Z0-9_]{1,15}[^/])*");
	private final Pattern LINKEDIN = Pattern.compile("http(s)?://(www.)?linkedin.com/(in)?(pub)?(groups)?/([a-zA-Z0-9_].*)");
	private final Pattern FACEBOOK = Pattern.compile("http(s)?://(www.)?facebook.com/(?!share(r)?.php)(?!media/)(?!photo.php)([a-zA-Z0-9_].*)");
	private final Pattern GOOGPLUS = Pattern.compile("http(s)?://plus.google.com/(?!share)([a-zA-Z0-9_].*)");
	private final Pattern EMAIL = Pattern.compile("^mailto:");
//	private PageCassandra cassandra;
	
	private static final Logger LOG = LoggerFactory.getLogger(URLDiscover.class);
	private final DecompressingHttpClient httpClient;
	
    public URLDiscover() {
        
    	cookieStore = new BasicCookieStore();
    	localContext = new BasicHttpContext();
    	visitedLinks = new HashSet<String>();
//    	cassandra = new PageCassandra();
        localContext.setAttribute(ClientContext.COOKIE_STORE, cookieStore);
        
        CookieHandler.setDefault(new CookieManager(null, CookiePolicy.ACCEPT_ALL));
		HttpParams httpsParams = new BasicHttpParams();
		HttpConnectionParams.setConnectionTimeout(httpsParams, 3000); 
        HttpConnectionParams.setSoTimeout(httpsParams, 3000);
        
        DefaultHttpClient baseHttpClient = URLDiscover.wrapClient(new DefaultHttpClient(httpsParams));
		baseHttpClient.setRedirectStrategy(new CustomRedirectStrategy());
		httpClient = new DecompressingHttpClient(baseHttpClient);
        httpClient.getParams().setParameter(CoreProtocolPNames.USER_AGENT, "Mozilla/5.0 (Windows; U; Windows NT 6.1; ja; rv:1.9.2a1pre) Gecko/20090403 Firefox/3.6a1pre");
    }
    
    public String getDomain(String url) {
        
        //System.out.println("**** Root Domain -> " + url);
        String domain = null;
        
        try {
        	String[] urlSplited = url.split("/");
        	if(urlSplited.length > 2) {
        		domain = urlSplited[2];
        	} else {
        		return null;
        	}
        	
        } catch(Exception e) { 
            LOG.error("Error'd in getDomain for " + url);
        }
        
        return domain;

    }
    
    public byte[] fetchDocumment(String inputUrl) {
		
		LOG.debug("Going to find links from url: " + inputUrl);
		
//		if(visitedLinks.contains(inputUrl)) {
//			return null;
//		} else {
//			visitedLinks.add(inputUrl);
//		}
		
		HttpGet method = new HttpGet(inputUrl);
        method.setHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
//        ArrayList<byte[]> ip_addrs = new ArrayList<byte[]>();
//        ip_addrs.add(new byte[]{ (byte)198, (byte)178, (byte)121, (byte)115 });
//        ip_addrs.add(new byte[]{ (byte)198, (byte)178, (byte)121, (byte)122 });
//        ip_addrs.add(new byte[]{ (byte)198, (byte)178, (byte)121, (byte)123 });
//        ip_addrs.add(new byte[]{ (byte)198, (byte)178, (byte)121, (byte)124 });
//        ip_addrs.add(new byte[]{ (byte)198, (byte)178, (byte)121, (byte)125 });
//        
//        Random rand = new Random();
//        ConnRouteParams.setLocalAddress(httpsParams, InetAddress.getByAddress(ip_addrs.get(rand.nextInt(ip_addrs.size()))));
//        
        
        try {
        	HttpResponse response = httpClient.execute(method,this.localContext);
            InputStream is = null;
        	is = response.getEntity().getContent();
        	ByteArrayOutputStream bos = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int nbRead;
            while ((nbRead = is.read(buffer)) != -1) {
                bos.write(buffer, 0, nbRead);
            }

            is.close();
            method.reset();
            method.releaseConnection();
            
            return bos.toByteArray();
            
		} catch (Exception ex) {
			// TODO: handle exception
			ex.printStackTrace();
			return null;
		}
        
        
//        httpClient.getConnectionManager().shutdown();
        
        
    }
    
    public static String getContent(String contentHtml) {
    	Document doc = Jsoup.parse(contentHtml);
//    	String cleaned = Jsoup.clean(contentHtml, Whitelist.none());
//    	return cleaned;
    	return doc.body().text();
    }
    
    public static String getCleanedHtml(String contentHtml) {
    	String cleaned = Jsoup.clean(contentHtml, Whitelist.none());
    	return cleaned;
    }
    
    public static int getContentLength(String contentHtml) {
    	String text = getCleanedHtml(contentHtml);
    	return text.length();
    }
    
    public static String getLinkMD5(String contentHtml) {
    	Document doc = Jsoup.parse(contentHtml);
    	if(doc == null) return null;
    	
    	StringBuilder strBuilder = new StringBuilder();
    	Elements links = doc.select("a");
    	for(Element link : links) {
    		String rel = link.attr("href");
    		if(rel != null && !rel.isEmpty()) {
    			strBuilder.append(rel);
    		}
    	}
    	
    	if(!strBuilder.toString().isEmpty()) {
    		return MD5Signature.getMD5(strBuilder.toString());
    	} else {
    		return null;
    	}
    }
    
	public Map<String, Object> findLinks(String parentUrl, String contentHtml) {
		
//		if(contentBytes == null) return null;
        
        Document doc = Jsoup.parse(contentHtml);
        
        if(doc == null) return null;
        
        Elements imports = doc.select("link");
        Elements list = doc.select("a[href]");
        
		List<String> urlList = new ArrayList<String>();
		List<String> rssList = new ArrayList<String>();
		List<String> socialList = new ArrayList<String>();
        
        for (Element link : imports) {
            if(	link.attr("rel") != null && 
            	link.attr("rel").toLowerCase().contains("altern") && 
            	link.attr("type").toLowerCase().contains("rss")) {
            	
            	String this_link = link.attr("abs:href");
               if(!visitedLinks.contains(this_link)) {
            	   visitedLinks.add(this_link);
            	   String itsDomain = getDomain(this_link);
            	   if(itsDomain != null) {
//            		   cassandra.insertFeed(itsDomain, link.attr("abs:href"));
            		   rssList.add(this_link);
            	   } 
               }
            }
        }
        
        for (Element link : list) {
            String this_link = link.attr("abs:href");                    

            if(this_link == null || this_link.isEmpty() )
                continue;                    

            // For debug...
            //System.out.println("Found URL on page: " + this_link);
            
            if (!visitedLinks.contains(this_link)) {
            	
            	visitedLinks.add(this_link);
                String linkLowered = this_link.toLowerCase();

                if(!FILTERS.matcher(linkLowered).matches() && !EMAIL.matcher(linkLowered).find()) {
//                	if(this_link.startsWith("http")) {
//                		urlList.add(this_link); 
//                	}else {
//                		addNewUrl(parentUrl, this_link);
//                	}
                	urlList.add(WebPage.cleanUrl(this_link)); 
                } 

                if(TWITTER.matcher(linkLowered).matches()) {
                    //System.out.println("**** Twitter URL found: " + this_link);
                	socialList.add("tw_" + WebPage.cleanUrl(this_link));
                } else if(FACEBOOK.matcher(linkLowered).matches()) {
                    //System.out.println("**** Facebook URL found: " + this_link);
                	socialList.add("fb_" + WebPage.cleanUrl(this_link));
                } else if(LINKEDIN.matcher(linkLowered).matches()) {
                    //System.out.println("**** LinkedIn URL found: " + this_link);
                	socialList.add("li_" + WebPage.cleanUrl(this_link));
                } else if(GOOGPLUS.matcher(linkLowered).matches()) {
                    //System.out.println("**** Google+ URL found: " + this_link);
                	socialList.add("gp_" + WebPage.cleanUrl(this_link));
                }
            }
        }
        
        Map<String,Object> foundLinks = new HashMap<String, Object>();
        foundLinks.put("url", urlList);
        foundLinks.put("rss", rssList);
        foundLinks.put("social", socialList);
//        foundLinks.put("rawHtml", doc.outerHtml());
        doc = null;
        return foundLinks;
	}
	
	public URL addNewUrl(String mainUrl, String relativeUrl){
		URL url;
		URL parentURL;
		try {
			parentURL = new URL(mainUrl);
		} catch (MalformedURLException e1) {
			e1.printStackTrace();
			return null;
		}
		try { 
			url = new URL(parentURL,relativeUrl);
			return url;
		} catch (MalformedURLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
    public static DefaultHttpClient wrapClient(HttpClient base) {
        try {
            SSLContext ctx = SSLContext.getInstance("TLS");
            X509TrustManager tm = new X509TrustManager() {
 
                public void checkClientTrusted(X509Certificate[] xcs, String string) throws CertificateException {
                }
 
                public void checkServerTrusted(X509Certificate[] xcs, String string) throws CertificateException {
                }
 
                public X509Certificate[] getAcceptedIssuers() {
                    return null;
                }
            };
            X509HostnameVerifier verifier = new X509HostnameVerifier() {
 
                public void verify(String string, SSLSocket ssls) throws IOException {
                }
 
                public void verify(String string, X509Certificate xc) throws SSLException {
                }
 
                public void verify(String string, String[] strings, String[] strings1) throws SSLException {
                }
 
                public boolean verify(String string, SSLSession ssls) {
                    return true;
                }
            };
            ctx.init(null, new TrustManager[]{tm}, null);
            SSLSocketFactory ssf = new SSLSocketFactory(ctx);
            ssf.setHostnameVerifier(verifier);
            ClientConnectionManager ccm = base.getConnectionManager();
            SchemeRegistry sr = ccm.getSchemeRegistry();
            sr.register(new Scheme("https", ssf, 443));
            
            return new DefaultHttpClient(ccm, base.getParams());
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }
    
    // test stuff
    @SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
    	String[] testURLs = {
//    			"http://techcrunch.com/enterprise/",
//    			"http://huff.to/17hKG3N",
//    			"http://envato.com/",
//    			"http://www.dreamincode.net/forums/topic/254818-how-to-determine-if-url-is-a-relative-url-and-fix-it/",
//    			"http://www.baidu.com/search/rss.html",
    			"https://news.google.com/"
    	};
    	
    	for (int i = 0; i < testURLs.length; i++) {
        	URLDiscover test = new URLDiscover();
        	
        	byte[] contentBytes = null;
        	
        	contentBytes = test.fetchDocumment(testURLs[i]);
        	
        	Map<String,Object> foundLinks = test.findLinks(testURLs[i], new String(contentBytes));
        	
        	List<String> links = (List<String>) foundLinks.get("url");
        	List<String> rss = (List<String>) foundLinks.get("rss");
        	List<String> social = (List<String>) foundLinks.get("social");
        	
        	String rawHtml = (String)foundLinks.get("rawHtml");
        	
        	System.out.println();
        	System.out.println("Test url: " + testURLs[i]);
        	System.out.println("Found urls:");
        	for(String link : links) {
        		System.out.println(link);
        	}
        	
        	System.out.println("Found rss:");
        	for(String link : rss) {
        		System.out.println(link);
        	}
        	
        	System.out.println("Found social links:");
        	for(String link : social) {
        		System.out.println(link);
        	}
        	
//        	System.out.println();
//        	System.out.println("=== Raw html is ===");        	
//        	System.out.println(rawHtml);
		}
    }
}
