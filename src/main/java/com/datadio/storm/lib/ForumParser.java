package com.datadio.storm.lib;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class ForumParser {
	private List<Map<String, String>> posts;
	private List<String> error_messages;
	private Document link_dom;
	private String forum_type;
	private String forum_version;
	private final String[] SUPPORT_TYPE = {"vBulletin", "SMF", "phpBB"};
	
	public ForumParser() {
		this.error_messages = new ArrayList<String>();
		this.posts = new ArrayList<Map<String, String>>();
		this.forum_type = null;
		this.forum_version = null;
	}
	
	public Document get_document(String url) throws IOException {
		Document doc = Jsoup.connect(url).get();
//		  .data("query", "Java")
//		  .userAgent("Mozilla")
//		  .cookie("auth", "token")
//		  .timeout(3000)
//		  .post();
		if(doc == null) {
			error_messages.add("Failed to parse html document for: " + url);
		}
		
		this.link_dom = doc;
		return this.link_dom;
	}
	
	public static Document getDocument(String url) {
		try {
			return Jsoup.connect(url).get();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	
	public void show_errors() {
		if(!this.error_messages.isEmpty()) {
			System.out.println("Ooops there are some errors:");
			for(String m : this.error_messages) {
				System.out.println(m);
			}
			System.out.println();
		} else {
			System.out.println("No errors have been catched.");
		}
	}
	
	public void print_results() {
		
		System.out.println();
		System.out.println("Detected Type is: " + this.forum_type);
		for(Map<String,String>post : this.posts) {
			
			System.out.println();
			System.out.println("Thread Title is: " + post.get("thread_title"));
			System.out.println("Post ID is: " + post.get("post_id"));
			System.out.println("Post date: " + post.get("post_date"));
			System.out.println("Post link: " + post.get("post_link"));
			System.out.println("User Name: " + post.get("user_name"));
			System.out.println("User Profile Link: " + post.get("user_profile_link"));
			System.out.println("User Extra Info: " + post.get("user_info"));
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
			System.out.println("Post Content: ");
			System.out.println(post.get("post_content"));
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		}
	}
	
	public boolean parseThread(String url) throws IOException {
		
		if(!detect_type(url)) {
			error_messages.add("Failed to detect the type of forum: " + url);
			return false;
		}
		
		if(forum_type.equals("vBulletin")) {
			if(forum_version != null) {
				if(forum_version.charAt(0) == '3') {
					return parse_post_vBulletin_V3(url);
				} else if(forum_version.charAt(0) == '4') {
					return parse_post_vBulletin_V4(url);
				}
			} else {
				return parse_post_vBulletin_V3(url);
			}
			
		} else if (forum_type.equals("SMF")) {
			return parse_post_SMF_V2(url);
			
		} else if (forum_type.equals("phpBB")) {
			return parse_post_phpBB(url);
		}
		
		return false;
	}
	
	public static boolean isForum(String url, String rawHtml) {
		Document doc = null;
		if(url != null) {
			doc = getDocument(url);
		} else {
			doc = Jsoup.parse(rawHtml);
		}

		if(doc == null) {
			return false;
		}
		
		Element ver_meta;
		String type = null;
//		long startTime = System.currentTimeMillis();
		if((ver_meta = doc.select("meta[name=generator]").first()) != null) {
			type = ver_meta.attr("content");
			if(type != null && type.contains("vBulletin")) {
				return true;
			}
		} else if((ver_meta = doc.select("a[href*=simplemachines.org]").first()) != null) {
			//SMF
			return true;
			
		} else if ((ver_meta = doc.select("a[href*=phpbb.com]").first()) != null) {
//			//phpBB
			return true;
			
		} else if ((ver_meta = doc.select("style#vbulletin_css").first()) != null) {
			//vBulletin
			return true;
			
		// not working
		} else if ((ver_meta = doc.select("body:contains(vBulletin_init)").first()) != null) {
			//vBulletin
			return true;
		} 
		
//		long endTime = System.currentTimeMillis();
//	    System.out.println("Total execution time: " + (endTime-startTime) + "ms");
		return false;
	}
	
	public void setDocFromString(String rawHtml) {
		Document doc = Jsoup.parse(rawHtml);
		this.link_dom = doc;
	}
	
	public boolean detect_type(byte[] htmlBytes) {
		String docStr = new String(htmlBytes);
		setDocFromString(docStr);
		try {
			return detect_type("");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	
	public boolean detect_type(String url) throws IOException {
		Document doc;
		if(this.link_dom != null) {
			doc = this.link_dom;
		} else {
			doc = get_document(url);
		}
		
		if(doc == null) {
			return false;
		}
		
		String arg[] = new String[2];
		Element ver_meta;
		String type = null;
//		long startTime = System.currentTimeMillis();
		if((ver_meta = doc.select("meta[name=generator]").first()) != null) {
			type = ver_meta.attr("content");
			if(type != null) {
				if(type.contains("vBulletin")) {
					arg = type.split(" ");
					this.forum_type = "vBulletin";
					if(arg.length > 1 && arg[1] != null) {
						this.forum_version = arg[1];
					}			
				}
			}
		} else if((ver_meta = doc.select("a[href*=simplemachines.org]").first()) != null) {
//			arg = ver_meta.ownText().split(" ");
//			arg[0] = "SMF";
			this.forum_type = "SMF";
		} else if ((ver_meta = doc.select("a[href*=phpbb.com]").first()) != null) {
//			arg = ver_meta.ownText().split(" ");
			this.forum_type = "phpBB";
		} else if ((ver_meta = doc.select("style#vbulletin_css").first()) != null) {
			this.forum_type = "vBulletin";
			
		// not working
		} else if ((ver_meta = doc.select("body:contains(vBulletin_init)").first()) != null) {
			this.forum_type = "vBulletin";
		} 
		
		else {
			return false;
		}
		
//		long endTime = System.currentTimeMillis();
//	    System.out.println("Total execution time: " + (endTime-startTime) + "ms");
		return true;
	}
	
	public boolean connect_vBulletin_V3(String url) throws IOException {
		
		Document doc;
		if(this.link_dom != null) {
			doc = this.link_dom;
		} else {
			doc = get_document(url);
		}
		
		if(doc == null) {
			return false;
		}

		System.out.println("Hello");
		Elements post_table = doc.select("[id^=threadbits_forum]");
		Elements posts = post_table.select("td[id^=td_threadtitle]");
//		for(Element t : posts) {
//			System.out.println("^^^^^^");
//			System.out.println(t);
//			System.out.println("^^^^^^");
//		}
		
        for (Element post : posts) {
        	Element post_stats = post.nextElementSibling();
        	Element link_container = post.select("div > a[href]").first();
        	String stats_title = post_stats.attr("title");
        	String[] stats = stats_title.split(",");
        	
        	System.out.println();
    		System.out.println("ID:" + link_container.id());
    		System.out.println("Title: " + link_container.ownText());
    		System.out.println("Link: " + link_container.attr("href"));
    		System.out.println("Post date:" + post_stats.text());
    		System.out.println(stats[0].trim());
    		System.out.println(stats[1].trim());
    		System.out.println();
        }
        

//       for (Element link : imports) {
//           if(link.attr("rel") != null && link.attr("rel").toLowerCase().contains("altern") && link.attr("type").toLowerCase().contains("rss")) {
//              if(!linkHandler.visited(link.attr("abs:href"))) {
//                  linkHandler.addVisited(link.attr("abs:href"));
//                  //System.out.println(" Feed => " + link.tagName() + " " + link.attr("abs:href") + " " + link.attr("rel"));
//                  cassandra.insertFeed(linkHandler.getDomain(url), link.attr("abs:href"));
//              }
//           }
//       }
		return true;
	}
	
	public boolean parse_post_vBulletin_V3(String url) throws IOException {
		Document doc;
		if(this.link_dom != null) {
			doc = this.link_dom;
		} else {
			doc = get_document(url);
		}
		
		if(doc == null) {
			return false;
		}
		
//		System.out.println("doc : " + doc);
		
		Element thread_title_dom = doc.select(".navbar > strong").first();
		
//		System.out.println("thread_title_dom : " + thread_title_dom);
		
		if(thread_title_dom == null) {
			thread_title_dom = doc.select("head title").first();
		}
		
//		Elements post_tables = doc.select("#posts table[id^=post]");
		Elements post_tables = doc.select("#posts table.tborder");
		
//		System.out.println("post_tables : " + post_tables);
		
		if(thread_title_dom == null || post_tables == null) {
			error_messages.add("Failed to apply vBulletin v3 parser to: " + url);
			return false;
		}
		
		String thread_title = thread_title_dom.ownText();
		
		for (Element post : post_tables) {
			String post_date = null;
			String post_link = null;
			String post_content = null;
			String post_id = null;
			
			String user_name = null;
			String user_info = null;
			String profile_link = null;
			
			if(post.id() != null && !post.id().isEmpty()) {
				post_id = StringUtils.replace(post.id(), "post", "");
			}	
			Elements sections = post.select("tr > td");
			
			int row_sec = 0;
			for (Element section : sections) {
				Element user_profile = null;
				Element post_container = null;
				boolean find_content = false;
				
				if(row_sec == 0) {
					post_date = section.text();
					if(post_id == null || post_id.isEmpty()) {
						post_id = section.select("a").first().attr("name");
						post_id = StringUtils.replace(post_id, "post", "");
					}
					
				}else if(row_sec == 1) {
					Element post_link_dom = section.select("a").first();
					if(post_link_dom != null) {
						post_link = post_link_dom.attr("href");
//						post_link = section.select("b > a").attr("href"); // some forum not have relative link (#)
					}
					
				}else {
					user_profile = section.select("a.bigusername").first();
					String sec_id = section.id();
					if (user_profile != null) {
						user_name = user_profile.text();
						profile_link = user_profile.attr("href");
						
						user_info = section.select(".smallfont").text();
						
					} else if (sec_id.equals("td_post_" + post_id)) {
						String message_id = "post_message_" + post_id;
						post_container = section.select('#'+message_id).first();
						System.out.println("here");
						if(post_container != null) {
							post_content = post_container.html().toString();
//							post_content = post_container.text();
							find_content = true;
						}
					} else if (!find_content) {
						post_container = section.select("div").last();
						
						if(post_container != null) {
							post_content = section.select("div").html().toString();
//							post_content = post_container.text();
							find_content = true;
						}
					}
				}
				row_sec++;
			}
			
			Map<String, String> this_post = new HashMap<String, String>();
			this_post.put("thread_title", thread_title);
			this_post.put("post_id", post_id);
			this_post.put("post_date", post_date);
			this_post.put("post_link", post_link);
			this_post.put("post_content", post_content);
			
			this_post.put("user_name", user_name);
			this_post.put("user_info", user_info);
			this_post.put("user_profile_link", profile_link);
			
			this.posts.add(this_post);
		
		}
		return true;
	}
	
	public boolean parse_post_vBulletin_V4(String url) throws IOException {
		Document doc;
		if(this.link_dom != null) {
			doc = this.link_dom;
		} else {
			doc = get_document(url);
		}
		
		if(doc == null) {
			return false;
		}
		
//		System.out.println("doc: \n" + doc);
		
		Element thread_title_dom = doc.select(".threadtitle > a").first();
		Elements post_tables = doc.select("ol#posts > li");
		
		if(thread_title_dom == null) {
			thread_title_dom = doc.select("head title").first();
		}
		
		if(thread_title_dom == null || post_tables == null) {
			error_messages.add("Failed to apply vBulletin v4 parser to: " + url);
			return false;
		}
		
		String thread_title = thread_title_dom.ownText();
		
		for (Element post : post_tables) {
			String post_date = null;
			String post_link = null;
			String post_content = null;
			String post_id = null;
			
			String user_name = null;
			String user_info = null;
			String profile_link = null;
			
//			System.out.println("Post: \n" + post);
			
			post_id = StringUtils.replace(post.id(), "post_", "");
			
			Element post_head_dom = post.select(".posthead").first();
			
			if(post_head_dom == null) {
				continue;
			}
			post_date = post_head_dom.select(".postdate .date").first().text();
			Element post_link_dom = post_head_dom.select("a.postcounter").first();
			
			if(post_link_dom != null) {
				post_link = post_link_dom.attr("href");
			}
			
			// because of javascript, these info might not obtainable.
			Element user_profile = post.select(".username_container > .memberaction").first();
			
			if(user_profile != null) {
				user_name = user_profile.select("a.username").text();
				profile_link = user_profile.select("a.username").attr("href");
				user_info = post.select(".userinfo_extra").first().text();
			} else {
				Element tmep_user_info_dom = post_head_dom.select(".xsaid a[href]").first();
				user_name = tmep_user_info_dom.ownText();
				profile_link = tmep_user_info_dom.attr("href");
			}
			
			String message_id = "post_message_" + post_id;
//			Element post_container = post.select('#'+message_id + " > .postcontent").first();
			Element post_container = post.select('#'+message_id).first();
			if(post_container != null) {
				post_content = post_container.html().toString();
//				post_content = post_container.text();
			}
			
			Map<String, String> this_post = new HashMap<String, String>();
			this_post.put("thread_title", thread_title);
			this_post.put("post_id", post_id);
			this_post.put("post_date", post_date);
			this_post.put("post_link", post_link);
			this_post.put("post_content", post_content);
			
			this_post.put("user_name", user_name);
			this_post.put("user_info", user_info);
			this_post.put("user_profile_link", profile_link);
			
			this.posts.add(this_post);
		}
		return true;
	}
	
	public boolean parse_post_phpBB_old(String url) throws IOException {
		Document doc;
		if(this.link_dom != null) {
			doc = this.link_dom;
		} else {
			doc = get_document(url);
		}
		
		if(doc == null) {
			return false;
		}
		
		Element thread_title_dom = doc.select("#pageheader a[href]").first();
		Elements post_tables = doc.select("#pagecontent .tablebg");
		
		if(thread_title_dom == null) {
			thread_title_dom = doc.select("head title").first();
		}
		
		if(thread_title_dom == null || post_tables == null) {
			error_messages.add("Failed to apply phpBB v3 2007 parser to: " + url);
			return false;
		}
		
		String thread_title =  thread_title_dom.ownText();
		
		for (Element post : post_tables) {
			
			String post_date = null;
			String post_link = null;
			String post_content = null;
			String post_id = null;
			
			String user_name = null;
			String user_info = null;
			String profile_link = null;
			
//			System.out.println("Post: \n" + post);
			Element post_author = post.select(".postauthor").first();
			
			if(post_author == null) {
				continue;
			}
			
			Element user_profile_dom = post.select(".profile > .postdetails").first();
			
			// div.postbody is post content, span.postbody is signature
			Element post_container = post.select("div.postbody").first();
			Element profile_link_dom = post.select("a[href*=memberlist]").first();
			Element post_id_dom = post_author.firstElementSibling();
			
			user_name = post_author.ownText();
			user_info = user_profile_dom.text();
			
			if(post_id_dom.hasAttr("name")) {
				post_id = post_id_dom.attr("name");
			}
			post_content = post_container.html().toString();
			profile_link = profile_link_dom.attr("href");
			
			Element post_link_dom = null;
			
			if(post_id != null) {
				post_link_dom = post.select("a[href*=" + post_id + "]").first();
			} else {
				post_link_dom = post.select("a[href~=(?i)(http|viewtopic)]").first();
			}
			
			post_link = post_link_dom.attr("href");
//			System.out.println("############# date dom /n");
//			System.out.println(post_link_dom.parent());
			post_date = post_link_dom.parent().ownText();
			
			Map<String, String> this_post = new HashMap<String, String>();
			this_post.put("thread_title", thread_title);
			this_post.put("post_id", post_id);
			this_post.put("post_date", post_date);
			this_post.put("post_link", post_link);
			this_post.put("post_content", post_content);
			
			this_post.put("user_name", user_name);
			this_post.put("user_info", user_info);
			this_post.put("user_profile_link", profile_link);
			
			this.posts.add(this_post);
		}
		
		return true;
	}
	
	public boolean parse_post_phpBB(String url) throws IOException {
		Document doc;
		if(this.link_dom != null) {
			doc = this.link_dom;
		} else {
			doc = get_document(url);
		}
		
		if(doc == null) {
			return false;
		}
		
		Element thread_body = doc.select("#page-body").first();
		
		if(thread_body == null) {
			error_messages.add("Failed to apply phpBB v3 parser to: " + url);
			return parse_post_phpBB_old(url);
		}
		
		Element thread_title_dom = thread_body.select("h2 a[href]").first();
		Elements post_tables = thread_body.select(".post");
		
		if(thread_title_dom == null) {
			thread_title_dom = doc.select("head title").first();
		}
		
		if(thread_title_dom == null || post_tables == null) {
			error_messages.add("Failed to apply phpBB v3 parser to: " + url);
			return false;
		}
		
		String thread_title =  thread_title_dom.ownText();
		
		for (Element post : post_tables) {
			
			String post_date = null;
			String post_link = null;
			String post_content = null;
			String post_id = null;
			
			String user_name = null;
			String user_info = null;
			String profile_link = null;
			
			Element user_profile = post.select(".postprofile").first();
			Element user_name_dom = user_profile.select("dt > a").first();
			Element post_body = post.select(".postbody").first();
			Element post_container = post_body.select(".content").first();
			Element post_author = post_body.select(".author").first();
			
			if(user_name_dom == null) {
				System.out.println("user profile link is missing under dt tag");
				System.out.println(user_profile);
				continue;
			}
			
			post_id = post.id();
			profile_link = user_name_dom.attr("href");
//			user_name = user_name_dom.ownText();
			user_name = user_profile.select("dt").text();
			user_info = user_profile.select("dd").text();
	
//			post_date = post_author.text();
			post_date = post_author.ownText();
			post_link = post_author.select("a[href]").first().attr("href");
			post_content = post_container.html().toString();
			
			Map<String, String> this_post = new HashMap<String, String>();
			this_post.put("thread_title", thread_title);
			this_post.put("post_id", post_id);
			this_post.put("post_date", post_date);
			this_post.put("post_link", post_link);
			this_post.put("post_content", post_content);
			
			this_post.put("user_name", user_name);
			this_post.put("user_info", user_info);
			this_post.put("user_profile_link", profile_link);
			
			this.posts.add(this_post);
		}
		
		return true;
	}
	
	public boolean parse_post_SMF_V1(String url) throws IOException {
		Document doc;
		if(this.link_dom != null) {
			doc = this.link_dom;
		} else {
			doc = get_document(url);
		}
		
		if(doc == null) {
			return false;
		}
		
		Elements post_tables = doc.select(".post");
		
		if(post_tables == null) {
			error_messages.add("Failed to apply SMF v1 parser to: " + url);
			return false;
		}
		
		Element thread_title_dom = doc.select("td#top_subject").first();
		
		
		if(thread_title_dom == null) {
			thread_title_dom = doc.select("head title").first();
		}
		
		if(thread_title_dom == null || post_tables == null) {
			error_messages.add("Failed to apply SMF v1 parser to: " + url);
			return false;
		}
		
		String thread_title =  thread_title_dom.ownText();
		
		for (Element post : post_tables) {
			
			String post_date = null;
			String post_link = null;
			String post_content = null;
			String post_id = null;
			
			String user_name = null;
			String user_info = null;
			String profile_link = null;
			
//			String user_signature = null;
			
			
			Element post_info = post.firstElementSibling();
			Element user_profile = post.parent().firstElementSibling();
			Element user_name_dom = user_profile.select("b > a").first();
//			Element signature_pom = user_profile.parent().nextElementSibling().select(".signature").first();
			Element post_title_dom = post_info.select("[id^=subject]").first();
			
			profile_link = user_name_dom.attr("href");
			user_name = user_name_dom.ownText();
			user_info = user_profile.select(".smalltext").text();
			
			post_id = post_title_dom.id();
			post_link = post_title_dom.select("a").first().attr("href");
			post_date = post_info.select(".smalltext").first().text();
			
			post_content = post.html().toString();
			
//			user_signature = signature_pom.html().toString();
			
			Map<String, String> this_post = new HashMap<String, String>();
			this_post.put("thread_title", thread_title);
			this_post.put("post_id", post_id);
			this_post.put("post_date", post_date);
			this_post.put("post_link", post_link);
			this_post.put("post_content", post_content);
			
			this_post.put("user_name", user_name);
			this_post.put("user_info", user_info);
			this_post.put("user_profile_link", profile_link);
			
			this.posts.add(this_post);
		}
		
		return true;
	}
	
	public boolean parse_post_SMF_V2(String url) throws IOException {
		Document doc;
		if(this.link_dom != null) {
			doc = this.link_dom;
		} else {
			doc = get_document(url);
		}
		
		if(doc == null) {
			return false;
		}
		
		Element thread_body = doc.select("#forumposts").first();
		
		if(thread_body == null) {
			error_messages.add("Failed to apply SMF v2 parser ( cannot find #forumposts body) to: " + url);
			return parse_post_SMF_V1(url);
		}
		
		Elements post_tables = thread_body.select(".post_wrapper");
		
		Element thread_title_dom = thread_body.select(".catbg").first();
		
		
		if(thread_title_dom == null) {
			thread_title_dom = doc.select("head title").first();
		}
		
		if(thread_title_dom == null || post_tables == null) {
			error_messages.add("Failed to apply SMF v2 parser to: " + url);
			return false;
		}
		
		String thread_title =  thread_title_dom.ownText();
		
		for (Element post : post_tables) {
			
			String post_date = null;
			String post_link = null;
			String post_content = null;
			String post_id = null;
			
			String user_name = null;
			String user_info = null;
			String profile_link = null;
			
//			String user_signature = null;
			
			
			Element user_profile = post.select(".poster").first();
			
			user_name = user_profile.select("h4").first().text(); // some may not have a[href]
			
			Element profile_link_dom = user_profile.select("h4 a").first();
			
			if(profile_link_dom != null) {
				profile_link = profile_link_dom.attr("href");
			}
			
			user_info = user_profile.select(".smalltext").text();
			
			Element post_content_dom = post.select(".postarea").first();
			
			Element post_link_dom = post_content_dom.select(".keyinfo a").first();
			
//			System.out.println("##### post_link_dom " + post_link_dom);
			post_id = post_link_dom.parent().attr("id");
			post_link = post_link_dom.attr("href");
			post_date = post_content_dom.select(".keyinfo .smalltext").first().ownText();
			post_content = post_content_dom.select(".post").first().text();
			
//			user_signature = signature_pom.html().toString();
			
			Map<String, String> this_post = new HashMap<String, String>();
			this_post.put("thread_title", thread_title);
			this_post.put("post_id", post_id);
			this_post.put("post_date", post_date);
			this_post.put("post_link", post_link);
			this_post.put("post_content", post_content);
			
			this_post.put("user_name", user_name);
			this_post.put("user_info", user_info);
			this_post.put("user_profile_link", profile_link);
			
			this.posts.add(this_post);
		}
		
		return true;
	}
	
	public static void main(String[] args) {
		
		String test_v3_1 = "http://www.wickedfire.com/shooting-shit/";
		String test_v3_2 = "http://www.civicforums.com/forums/197-mechanical-problems-vehicle-issues-fix-forum";
		String test_v3_3 = "http://boards.houstontexans.com/forumdisplay.php?f=7";
		
		// version 3.0 (< 3.5)
		String post_v3_old_1 = "http://www.bigskyfishing.com/phpbb2/showthread.php?t=1725";
		String post_v3_old_2 = "http://www.ultimate-guitar.com/forum/showthread.php?t=1611720";
		
		// has javascript
		String post_v3_old_3 = "http://www.ngycp.org/vBulletin/showthread.php?t=137932";
		
		String post_v3_1 = "http://www.wickedfire.com/shooting-shit/174998-do-you-use-tor-all-lube-er-up.html";
		String post_v3_2 = "http://www.civicforums.com/forums/197-mechanical-problems-vehicle-issues-fix-forum/356354-technical-issues-my-97-dx-need-some-help-advice.html";
		String post_v3_3 = "http://boards.houstontexans.com/showthread.php?t=37473";
		String post_v3_4 = "http://www.fitday.com/fitness/forums/fitday-pc/9928-how-do-you-totally-erase-weight-entry.html";
		String post_v3_5 = "http://www.cpurigs.com/forums/showthread.php?t=3708";
		String post_v3_6 = "http://www.blackhatworld.com/blackhat-seo/black-hat-seo-tools/558519-get-tumbstudio-free-tumblr-bot-manage-multiple-accounts.html";
		String post_v3_7 = "http://forums.anandtech.com/showthread.php?t=2334434";
		String post_v3_8 = "http://www.flutrackers.com/forum/showthread.php?t=176535";
		String post_v3_9 = "http://www.v7n.com/forums/forum-management/345233-apart-phpbb-what-forum.html#post2031089";
		String post_v3_10 = "http://www.vbulletin.org/forum/showthread.php?t=204803";
		
		String post_v4_1 = "http://www.zyngaplayerforums.com/showthread.php?1646448-The-Ville-discontinued"; // js problem!!
		String post_v4_2 = "http://mmofuse.net/forums/f11/single-play-wow-repack-3649/";
		String post_v4_3 = "http://www.completevb.com/pre-sales-questions/2291-skins-xenforo.html";
		// structure not the same in posthead
		String post_v4_4 = "http://www.scrapmetalforum.com/electronics-recycling/20213-have-vme-system-parts-anywhere-sell-them.html";
		String post_v4_5 = "http://forum.bodybuilding.com/showthread.php?t=146571333";
		String post_v4_6 = "http://forum.xda-developers.com/showthread.php?t=2230428";
		String post_v4_7 = "http://ubuntuforums.org/showthread.php?t=2165622";
		String post_v4_8 = "https://forums.pocketgems.com/showthread.php?26483-Version-3.2";
		String post_v4_9 = "http://forum.applian.com/showthread.php?11196-Where-is-V-3.2.0.3";
		String post_v4_10 = "http://blackhatforum.com/showthread.php?6-How-to-Increase-Your-Clickthrough-Rates-on-Comment-Posting-Links";
		
		String phpBB_url = "http://forums.mozillazine.org/viewtopic.php?f=38&t=2735837&sid=87a5e38cf0b85c08c2a9c447a7781af1";
		// not working, missing profile link
		String phpBB_url2 = "http://www.phpbbhacks.com/forums/what-are-you-listening-to-right-now-vt55477.html";
		// return 503
		String phpBB_url3 = "https://www.phpbb.com/community/viewtopic.php?f=46&t=2192713";
		String phpBB_url4 = "http://belcandidat.freeforums.org/greece-to-see-nationwide-anti-austerity-strike-t3228.html";
		String phpBB_url5 = "http://forum.thinkpads.com/viewtopic.php?f=28&t=108155&start=90#p714557";
		String phpBB_url6 = "http://www.offsetguitars.com/forums/viewtopic.php?f=41&t=70185";
		String phpBB_url7 = "http://www.cleanyourcar.co.uk/forum/viewtopic.php?f=20&t=20096";
		String phpBB_url8 = "http://olympusdkteam.dk/viewtopic.php?f=9&t=1054";
		
		String smf_url1 = "http://festoolownersgroup.com/festool-tool-reviews/side-by-side-comparison-festool-kapex-on-kapex-ug-v-dewaltdewalt/";
		String smf_url2 = "http://www.majorspoilers.com/smf/index.php/topic,79.0.html";
		String smf_url3 = "http://www.solarvps.com/forums/index.php/topic,768.0.html";
		String smf_url4 = "http://www.simplemachines.org/community/index.php?topic=497288";
		
		String unknown_1 = "http://www.neogaf.com/forum/showthread.php?t=644307";
		String not_working_2 = "http://www.svtperformance.com/forums/road-side-pub-17/955724-nismo-r35-gt-r-break-production-car-0-60mph-record-2-0-seconds.html";
		
		System.out.println("Is forum?" + ForumParser.isForum(phpBB_url, null));
		
//		ForumParser vb = new ForumParser();
//		try {
////			vb.connect_vBulletin_V3(test_v3_1);
////			vb.detect_type(smf_url1);
////			vb.parse_post_vBulletin_V3("http://www.ultimate-guitar.com/forum/showthread.php?t=1611720");
////			vb.parse_post_vBulletin_V4(post_v4_2);
////			vb.parse_post_phpBB("http://www.scrapmetalforum.com/electronics-recycling/20213-have-vme-system-parts-anywhere-sell-them.html");
////			vb.parse_post_SMF_V2(smf_url3);
////			vb.parse_post_phpBB_old(phpBB_url4);
////			vb.print_results();
////			vb.show_errors();
////			
//			if(vb.parseThread("http://www.sitepoint.com/forums/showthread.php?584344-Screen-scraping-forums")) {
//				vb.print_results();
//			} else {
//				vb.show_errors();
//			}
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}
}
