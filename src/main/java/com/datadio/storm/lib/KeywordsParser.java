package com.datadio.storm.lib;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//import com.ibm.icu.text.UnicodeSet;

import org.apache.commons.lang.StringUtils;

/*
 * Given tweet text, check if it matches those keywords for certain project.
 */
public class KeywordsParser {
	Map<Integer, List<String>> term_container;
	Map<Integer, Integer> total_kwds_container;
	
	public KeywordsParser(Map<Integer, List<String>> term_container) {
		this.term_container = term_container;
		this.total_kwds_container = new HashMap<Integer, Integer>();
	}
	
	/**
	 * @param text - a string of tweet
	 * @return a Map object that each <k,v> contains the matched project id and tagged keywords.
	 */
	public Map<Integer, List<String>> get_matched_projects(String text) {
		List<String> tagged_kwds;
		Map<Integer, List<String>> matched_projects = new HashMap<Integer, List<String>>();
		text = text.toLowerCase();
    	for(Integer key : this.term_container.keySet()) {
//			System.out.println("Twitter tracked keywords for project" + key.toString());
    		tagged_kwds = new ArrayList<String>();
			
			for(String keyword : this.term_container.get(key)){
//				String new_keyword = keyword.replaceAll("\\s", "( |-|)");
//	            Pattern match_pattern = Pattern.compile("(?iu)(^|\\b|\\s)(" + new_keyword + ")(\\b|$)");
//	            Matcher matcher = match_pattern.matcher(text);
//	            if (matcher.find()) {
//	            	tagged_kwds.add(keyword);
//	            }
	            
//	            UnicodeSet match_pattern = new UnicodeSet("(^|\\b|\\s)(" + new_keyword + ")(\\b|$)", UnicodeSet.CASE_INSENSITIVE);
//	            boolean matcher = match_pattern.containsAll(text);
//	            if(matcher) {
//	            	tagged_kwds.add(keyword);
//	            }
			
//				String new_keyword = keyword.replaceAll("\\s", "-");
//				String new_keyword2 = keyword.replaceAll("\\s", "");
				String new_keyword = StringUtils.replace(keyword, "\\s", "-");
				String new_keyword2 = StringUtils.replace(keyword, "\\s", "");
				if(text.contains(keyword.toLowerCase()) || text.contains(new_keyword.toLowerCase()) || text.contains(new_keyword2.toLowerCase())) {
	            	tagged_kwds.add(keyword);
	            }
	    	}
			
			if(!tagged_kwds.isEmpty()) {
				matched_projects.put(key, tagged_kwds);
				Integer total_kwds = this.term_container.get(key).size();
				this.total_kwds_container.put(key, total_kwds);
			}
		}
    	
    	return matched_projects;
	}
	
	/**
	 * @return: a Map object that each <k,v> contains the matched project id and total number of keywords that the project contains.
	 */
	public Map<Integer, Integer> get_total_keywords_count() {
		return this.total_kwds_container;
	}
}
