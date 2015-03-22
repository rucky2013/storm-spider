package com.datadio.storm.lib;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datadio.storm.fetcher.URLResolver;
import com.google.common.collect.ImmutableMap;

public class LinkCleaner {
	
//	private final static int[] unSafeCharNum = {32,35,60,62,91,92,93,94,96,123,124,125,126};
	// http://www.tutorialspoint.com/html/html_url_encoding.htm
	static final Map<String, String> unSafeChar = ImmutableMap.<String, String>builder()
			.put(" ", "%20").put("#", "%23").put("<", "%3c").put(">", "%3e")
			.put("[", "%5b").put("\\", "%5c").put("]", "%5d").put("^", "%5e")
			.put("`", "%60").put("{", "%7b").put("|", "%7c").put("}", "%7d")
			.put("~", "%7e").put("\r", "").put("\n", "").build();
	
	private static final Logger LOG = LoggerFactory.getLogger(LinkCleaner.class);
   
	public static String getCanonicalURL(String url) {
		return getCanonicalURL(url, null);
	}

	public static String getCanonicalURL(String inputHref, String context) {
		URL result = null;
		String href = urlEncodeReplace(inputHref.trim());

//		Error'd in getDomain for mailto:cnntvsales@turner.com
		try {
			URL canonicalURL = new URL(URLResolver.resolveUrl(context == null ? "" : context, href));

			String path = canonicalURL.getPath();
			
			// url with extra quotes http://slide.sh.sina.com.cn/fashion/slide_19_3367_166767.html%23p=1"
			if(path.endsWith("\"")) {
				if(path.startsWith("\"")) {
					path = path.substring(1, path.length()-1);
				} else {
					path = path.substring(0, path.length()-1);
				}
			}

			/*
			 * Convert '//' -> '/'
			 */
			int idx = path.indexOf("//");
			while (idx >= 0) {
				path = path.replace("//", "/");
				idx = path.indexOf("//");
			}
			
			/*
			 * Normalize: no empty segments (i.e., "//"), no segments equal to
			 * ".", and no segments equal to ".." that are preceded by a segment
			 * not equal to "..".
			 */
//			path = new URI(path).normalize().toString();
			/*
			 * Drop starting '/../'
			 */
			while (path.startsWith("/../")) {
				path = path.substring(3);
			}
			

			/*
			 * Trim
			 */
			path = path.trim();

			final SortedMap<String, String> params = createParameterMap(canonicalURL.getQuery());
			final String queryString;

			if (params != null && params.size() > 0) {
				String canonicalParams = canonicalize(params);
				queryString = (canonicalParams.isEmpty() ? "" : "?" + canonicalParams);
			} else {
				queryString = "";
			}

			/*
			 * Add starting slash if needed
			 */
			if (path.length() == 0) {
				path = "/" + path;
			}

			/*
			 * Drop default port: example.com:80 -> example.com
			 */
			int port = canonicalURL.getPort();
			if (port == canonicalURL.getDefaultPort()) {
				port = -1;
			}

			/*
			 * Lowercasing protocol and host
			 */
			String protocol = canonicalURL.getProtocol().toLowerCase();
			String host = canonicalURL.getHost().toLowerCase();
			String pathAndQueryString = normalizePath(path) + queryString;

			result = new URL(protocol, host, port, pathAndQueryString);
			
			return result.toExternalForm().toLowerCase();
			
		} catch (MalformedURLException ex) {
//			System.out.println("######## The Original URL: " + inputHref);
//			System.out.println("######## The Safe URL: " + href);

			LOG.error("Incorrect URL format, the original URL: {}", inputHref);
			LOG.error(ex.getMessage(), ex);
			return href.toLowerCase();
			
		} catch (Exception ex) {
			LOG.error(ex.getMessage(), ex);
			return href.toLowerCase();
		}
		
	}
        
        /**
	 * Takes a query string, separates the constituent name-value pairs, and
	 * stores them in a SortedMap ordered by lexicographical order.
	 * 
	 * @return Null if there is no query string.
	 */
	private static SortedMap<String, String> createParameterMap(final String queryString) {
		if (queryString == null || queryString.isEmpty()) {
			return null;
		}

		final String[] pairs = queryString.split("&");
		final Map<String, String> params = new HashMap<String, String>(pairs.length);

		for (final String pair : pairs) {
			if (pair.length() == 0) {
				continue;
			}

			String[] tokens = pair.split("=", 2);
			switch (tokens.length) {
			case 1:
				if (pair.charAt(0) == '=') {
					params.put("", tokens[0]);
				} else {
					params.put(tokens[0], "");
				}
				break;
			case 2: 
				params.put(tokens[0], tokens[1]);
				break;
			}
		}
		return new TreeMap<String, String>(params);
	}
        
    private static String canonicalize(final SortedMap<String, String> sortedParamMap) {
		if (sortedParamMap == null || sortedParamMap.isEmpty()) {
			return "";
		}

		final StringBuffer sb = new StringBuffer(100);
		for (Map.Entry<String, String> pair : sortedParamMap.entrySet()) {
			final String key = pair.getKey().toLowerCase();
			if (key.equals("jsessionid") || key.equals("phpsessid") || key.equals("aspsessionid")) {
				continue;
			}
			if (sb.length() > 0) {
				sb.append('&');
			}
			sb.append(percentEncodeRfc3986(pair.getKey()));
			if (!pair.getValue().isEmpty()) {
				sb.append('=');
				sb.append(percentEncodeRfc3986(pair.getValue()));
			}
		}
		return sb.toString();
	
    }
        
       /**
	 * Percent-encode values according the RFC 3986. The built-in Java
	 * URLEncoder does not encode according to the RFC, so we make the extra
	 * replacements.
	 * 
	 * @param string
	 *            Decoded string.
	 * @return Encoded string per RFC 3986.
	 */
    private static String percentEncodeRfc3986(String string) {
		try {
			string = string.replace("+", "%2B");
			string = URLDecoder.decode(string, "UTF-8");
			string = URLEncoder.encode(string, "UTF-8");
//			return string.replace("+", "%20").replace("*", "%2A").replace("%7E", "~");
			return string;
		} catch (Exception e) {
			return string;
		}
	}
    
    public static String urlEncodeReplace(String input) {
    	for(Map.Entry<String, String>rule : unSafeChar.entrySet()) {
    		input = StringUtils.replace(input, rule.getKey(), rule.getValue());
    	}
    	return input;
    }
    
    public static String urlEncodeReplaceOld(String input) {
    	for(Map.Entry<String, String>rule : unSafeChar.entrySet()) {
    		input = input.replace(rule.getKey(), rule.getValue());
    	}
    	return input;
    }
    
    // will replace http:// with % char
    public static String urlCharReplace(String input) {
        StringBuilder resultStr = new StringBuilder();
        for (char ch : input.toCharArray()) {
        	String str = Character.toString(ch);
            if (unSafeChar.containsKey(str)) {
            	resultStr.append(unSafeChar.get(str));
            } else {
                resultStr.append(ch);
            }
        }
        return resultStr.toString();
    }

	private static String normalizePath(final String path) {
		return path.replace("%7E", "~").replace(" ", "%20");
	}
	
	// test stuff
	public static void main(String[] args) throws Exception {
		String[] testURLs = {
				"http://ad2.netshelter.net/jump/ns.dreamincode/~general;ppos=atf;kw=;tile=2|sz=300x250,300x600|ord=123456789?",
				"http://www.dreamincode.net/forums/index.php?app=forums&module=<post&section>=post&do=reply post&f=32&t=254818&qpid=1481567&s=d034a21c730f142352b85f9ba7381b4c",
				"http://ad.doubleclick.net/jump/wedding_bee/blog;kw=bottom;pos=^3;wb_date=\\u;sect=insert section name;tile=^3;[sz=300x250;ord=123456789]",
				"http://ad-emea.doubleclick.net/ddm/trackclk/N4472.274341.SINTAGESPAREAS.GR1/B7798085.2;dc_trk_aid=274410611;dc_trk_cid=55212642;ord=[timestamp]?http://www.sintagespareas.gr/sintages/category/i-knorr-proteinei.html#sthash.vRk3AJAo.dpuf"
				};
		
		System.out.println("Start testing url unsafe char replace: ");
		
		for (int i = 0; i < testURLs.length; i++) {
			System.out.println("Apac: " + LinkCleaner.urlEncodeReplace(testURLs[i]));
			System.out.println("Loop: " + LinkCleaner.urlCharReplace(testURLs[i]));
		}
		
		System.out.println("Time testing: ");
		
		long startTime = System.currentTimeMillis();
		for(int n = 0; n < 100000; n++) {
			for (int i = 0; i < testURLs.length; i++) {
				LinkCleaner.urlEncodeReplace(testURLs[i]);
			}
		}
		long endTime = System.currentTimeMillis();
		long spent = endTime - startTime;
		System.out.println("Time spent for apache: " + spent);
		
		long startTime2 = System.currentTimeMillis();
		for(int n = 0; n < 100000; n++) {
			for (int i = 0; i < testURLs.length; i++) {
				LinkCleaner.urlCharReplace(testURLs[i]);
			}
		}
		long endTime2 = System.currentTimeMillis();
		long spent2 = endTime2 - startTime2;
		System.out.println("Time spent for char loop: " + spent2);
		
		long startTime3 = System.currentTimeMillis();
		for(int n = 0; n < 100000; n++) {
			for (int i = 0; i < testURLs.length; i++) {
				LinkCleaner.urlEncodeReplaceOld(testURLs[i]);
			}
		}
		long endTime3 = System.currentTimeMillis();
		long spent3 = endTime3 - startTime3;
		System.out.println("Time spent for char loop: " + spent3);
		
//		long startTime = System.currentTimeMillis();
////		for(int n = 0; n < 100000; n++) {
			for (int i = 0; i < testURLs.length; i++) {
				String cleanedUrl = LinkCleaner.getCanonicalURL(testURLs[i]);
				System.out.println("Input URL: " + testURLs[i]);
				System.out.println("Output URL: " + cleanedUrl);
			}
////		}
//		long endTime = System.currentTimeMillis();
//		long spent = endTime - startTime;
//		System.out.println("Time spent: " + spent);
			
		String badUrl = "http://injustice.pubarticles.com/";
		String newUrl = LinkCleaner.getCanonicalURL(badUrl);
		System.out.println("Returned new url:" + newUrl);
	}
        
}
