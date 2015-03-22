package com.datadio.storm.lib;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class AdvancedFilter {
	private final String[] ACTION_TYPE = {"Modify score", "Add to list", "Set alert"};
	private final String filter_query = "SELECT filters.id, subjects.subject_type, contains.* FROM filters JOIN subjects ON subjects.filter_id = filters.id JOIN contains ON contains.subject_id = subjects.id WHERE filters.project_id = ?";
//	private Connection con = null;
//    private Statement st = null;
	private ComboPooledDataSource cp_pool = null;
	private SimpleCache<Integer, Map<String, Map<String, Map<String, List<String>>>>> filterCache;
	private static final Logger LOG = LoggerFactory.getLogger(AdvancedFilter.class);
	// cache expire time
	private int secondsToLive = 3600;
	
    public AdvancedFilter() {
    	this("jdbc:mysql://localhost:3306", "datadio", "datadio", "j38h78237fg32");
    }
    
    public AdvancedFilter(String url, String database, String user, String password) {
    	// use c3p0 pool
    	cp_pool = new ComboPooledDataSource();
    	try {
//			cp_pool.setDriverClass( "com.mysql.jdbc.Driver" );
    		cp_pool.setDriverClass( "org.mariadb.jdbc.Driver" );
		} catch (PropertyVetoException e) {
			e.printStackTrace();
		} //loads the jdbc driver            
    	cp_pool.setJdbcUrl( url + "/" + database );
    	cp_pool.setUser(user);                                  
    	cp_pool.setPassword(password);                                  
    	cp_pool.setMaxStatements(300);
    	cp_pool.setMaxConnectionAge(240);
        
        filterCache = new SimpleCache<Integer, Map<String, Map<String, Map<String, List<String>>>>>(200);
    }
    
    /**
     * Full wrap of filters for the given project.
     * @param project_id
     * @return
     * @throws SQLException
     */
    public Map<String, Map<String, Map<String, List<String>>>> get_filters(int project_id) throws SQLException {
//    	project_id = 24;
    	Connection con = null;
    	PreparedStatement filter_st = null;
    	ResultSet filters = null;
    	
    	try {
    		con = this.cp_pool.getConnection();
    	
	    	filter_st = con.prepareStatement(filter_query);
	    	filter_st.setString(1, Integer.toString(project_id));
	    	filters = filter_st.executeQuery();
	    	String filter_subject_name;
	    	Map<String, Map<String, Map<String, List<String>>>> filter_container = new HashMap<String, Map<String, Map<String, List<String>>>>();
	    	Map<String, Map<String, List<String>>> subject_container;
	    	Map<String, List<String>> content_container;
			List<String> conj_container;
			
	    	while(filters.next()) {
	    		filter_subject_name = Integer.toString(filters.getInt("filters.id")) + ":" + filters.getString("subjects.subject_type");
	    		
	    		String sub_type = filters.getString("contains.sub_type");
	    		Integer logic = filters.getInt("contains.logic");
	    		Integer conjunction = filters.getInt("contains.conjunction");
	    		String map_name;
	    		String content;
	    		
	    		if(!filter_container.isEmpty() && filter_container.get(filter_subject_name) != null) {
	    			subject_container = filter_container.get(filter_subject_name);	
				} else {
					subject_container = new HashMap<String, Map<String, List<String>>>();
				}
	    		
	    		if(subject_container != null && !subject_container.isEmpty() && subject_container.get(sub_type) != null) {
					content_container = subject_container.get(sub_type);		
				} else {
					content_container = new HashMap<String, List<String>>();
				}
	    		
	    		
	    		if(sub_type == "sentiment") {
					map_name =  logic.toString();
				} else {
					map_name =  logic.toString() + ":" + conjunction.toString();
				}
				
	    		conj_container = new ArrayList<String>();
	    		
				if(content_container != null && !content_container.isEmpty() && content_container.get(map_name) != null) {
					conj_container = content_container.get(map_name);		
				} else {
					conj_container = new ArrayList<String>();
				}
				
				if(sub_type == "sentiment") {
					conj_container.add(conjunction.toString());
					
				} else {
					content = filters.getString("contains.content");
					conj_container.add(content);
				}
				
				content_container.put(map_name, conj_container);
				subject_container.put(sub_type, content_container);
				filter_container.put(filter_subject_name, subject_container);
	
	    	}
    	
	    	return filter_container;
	    	
    	} catch (SQLException e) {
			LOG.error("Failed to get filters from MySQL for project: " + String.valueOf(project_id));
			LOG.error(e.getMessage(), e);
			return null;
			
		} finally {
			if (filters != null) try { filters.close(); } catch (SQLException quiet) {}
			if (filter_st != null) try { filter_st.close(); } catch (SQLException quiet) {}
			if (con != null) try { con.close(); } catch (SQLException quiet) {}
		}
    }
    
    /**
     * Old version of get filters, that use multiply queries.
     * @param project_id
     * @return
     * @throws SQLException
     */
    public Map<String, Map<String, Map<String, List<String>>>> get_filters_split_query(int project_id) throws SQLException {
    	
    	Connection con = null;
    	PreparedStatement filter_st = null;
    	ResultSet filters = null;
    	
    	try {
    		con = this.cp_pool.getConnection();
	    	filter_st = con.prepareStatement("SELECT id FROM `filters`  WHERE `filters`.`project_id` = " + project_id);
	    	filters = filter_st.executeQuery();
	    	ResultSet subjects;
	    	String subject_type;
	    	String filter_subject_name;
	    	Map<String, Map<String, List<String>>> subject_container;
	    	Map<String, Map<String, Map<String, List<String>>>> filter_container = new HashMap<String, Map<String, Map<String, List<String>>>>();
	    	
	    	while(filters.next()) {
	//    		System.out.println("");
	//    		
	//    		System.out.println("@@@@@@@@@@@@@ Starting Query Subject typ");
	    		
	    		PreparedStatement subject_st = con.prepareStatement("SELECT id, subject_type FROM `subjects` WHERE `subjects`.`filter_id` = " + filters.getInt("id"));
	    		subjects = subject_st.executeQuery();
	//    		SELECT id, subject_type FROM `subjects` WHERE `subjects`.`filter_id` IN (SELECT `id` FROM `filters` WHERE `filters`.`project_id` = pid)
	    		
	    		while(subjects.next()) {
	//    			System.out.println("@@@@@@@@@@@@@ Found Query Subject type");
	    			
	    			subject_type = subjects.getString("subject_type");
	    			subject_container = get_subject_container(subjects.getInt("id"));
	    			filter_subject_name = Integer.toString(filters.getInt("id")) + ":" + subject_type;
	    			filter_container.put(filter_subject_name, subject_container);
	    		}
	    		
	    		subject_st.close();
	    	}
	    	return filter_container;
	    	
    	} catch (SQLException e) {
			LOG.error("Failed to get filters from MySQL for project: " + String.valueOf(project_id));
			LOG.error(e.getMessage(), e);
			return null;
			
		} finally {
			if (filter_st != null) try { filter_st.close(); } catch (SQLException quiet) {}
			if (filters != null) try { filters.close(); } catch (SQLException quiet) {}
			if (con != null) try { con.close(); } catch (SQLException quiet) {}
		}
    }
    
    public Map<String, Map<String, List<String>>> get_subject_container(int subject_id) throws SQLException {
    	String sub_type;
    	String content;
    	Integer logic;
    	Integer conjunction;
    	
    	Connection con = null;
    	PreparedStatement contain_st = null;
    	ResultSet contains = null;
    	
    	try {
    		con = this.cp_pool.getConnection();
	    	contain_st = con.prepareStatement("SELECT `contains`.* FROM `contains` WHERE `contains`.`subject_id` = " + subject_id);
	    	contains = contain_st.executeQuery();
	    	
	    	Map<String, Map<String, List<String>>> subject_container = new HashMap<String, Map<String, List<String>>>();
			
			while(contains.next()) {
				sub_type = contains.getString("sub_type");
				logic = contains.getInt("logic");
				conjunction = contains.getInt("conjunction");
				
				Map<String, List<String>> content_container;
				String map_name;
				List<String> conj_container;
				
				if(subject_container.get(sub_type) != null) {
					content_container = subject_container.get(sub_type);		
				} else {
					content_container = new HashMap<String, List<String>>();
				}
				
				if(sub_type == "sentiment") {
					map_name =  logic.toString();
				} else {
					map_name =  logic.toString() + ":" + conjunction.toString();
				}
				
				if(content_container.get(map_name) != null) {
					conj_container = content_container.get(map_name);		
				} else {
					conj_container = new ArrayList<String>();
				}
				
				if(sub_type == "sentiment") {
					conj_container.add(conjunction.toString());
					
				} else {
					content = contains.getString("content");
					conj_container.add(content);
				}
				
				content_container.put(map_name, conj_container);
				subject_container.put(sub_type, content_container);
			}
			
			return subject_container;
    	} catch (SQLException e) {
			LOG.error("Failed to get subject from MySQL. Subject id : " + String.valueOf(subject_id));
			LOG.error(e.getMessage(), e);
			return null;
			
		} finally {
			if (contains != null) try { contains.close(); } catch (SQLException quiet) {}
			if (contain_st != null) try { contain_st.close(); } catch (SQLException quiet) {}
			if (con != null) try { con.close(); } catch (SQLException quiet) {}
		}
    }
    
    public boolean check_by_term(List<String>entry_terms, Map<String, List<String>>term_container) {
    	//logic.toString() + ":" + conjunction.toString();
    	String and_include = "1:1";
    	String and_exclude = "1:-1";
    	String or_include = "0:1";
    	String or_exclude = "0:-1";
    	
    	Set<String> term_set;
    	Set<String> entry_term_set = new HashSet<String>(entry_terms);
    	
    	// must include ALL terms
    	if(term_container.containsKey(and_include)) {
    		term_set = new HashSet<String>(term_container.get(and_include));
    		if(!term_set.equals(entry_term_set)) {
    			return false;
    		}
    	}
    	
    	// must exclude ALL terms
    	if(term_container.containsKey(and_exclude)) {
    		term_set = new HashSet<String>(term_container.get(and_exclude));
    		if(term_set.removeAll(entry_term_set)) {
    			return false;
    		}
    	}
    	
    	// must include AT LEAST ONE term
    	if(term_container.containsKey(or_include)) {
    		term_set = new HashSet<String>(term_container.get(or_include));
    		if(!term_set.removeAll(entry_term_set)) {
    			return false;
    		}
    	}
    	
    	// must exclude AT LEAST ONE term
    	if(term_container.containsKey(or_exclude)) {
    		term_set = new HashSet<String>(term_container.get(or_exclude));
    		if(term_set.equals(entry_term_set)) {
    			return false;
    		}
    	}
    	
    	return true;
    }
    
    public boolean check_by_content(String entry_content, Map<String, List<String>>term_container) {

    	String and_include = "1:1";
    	String and_exclude = "1:-1";
    	String or_include = "0:1";
    	String or_exclude = "0:-1";
    	List<String> filter_terms;
    	
    	if(term_container.containsKey(and_include)) {
    		filter_terms = term_container.get(and_include);
    		for (String term : filter_terms) {
//    			String new_term = term.replaceAll("\\s", "( |-|)");
//    	        Pattern match_pattern = Pattern.compile("(?iu)(^|\\b|\\s)(" + new_term + ")(\\b|$)");
//    	        Matcher matcher = match_pattern.matcher(entry_content);
//    	        if (!matcher.find()) {
//    	        	return false;
//    	        }
    	        
    	        entry_content = entry_content.toLowerCase();
//				String new_keyword = term.replaceAll("\\s", "-");
//				String new_keyword2 = term.replaceAll("\\s", "");
				
				String new_keyword = StringUtils.replace(term, "\\s", "-");
				String new_keyword2 = StringUtils.replace(term, "\\s", "");
				if(!entry_content.contains(term.toLowerCase()) && 
						!entry_content.contains(new_keyword.toLowerCase()) && 
						!entry_content.contains(new_keyword2.toLowerCase())) {
					return false;
	            }
    		}
    	}
    	
    	if(term_container.containsKey(and_exclude)) {
    		filter_terms = term_container.get(and_exclude);
    		for (String term : filter_terms) {
//    			String new_term = term.replaceAll("\\s", "( |-|)");
//    	        Pattern match_pattern = Pattern.compile("(?iu)(^|\\b|\\s)(" + new_term + ")(\\b|$)");
//    	        Matcher matcher = match_pattern.matcher(entry_content);
//    	        if (matcher.find()) {
//    	        	return false;
//    	        }
    	        
    	        entry_content = entry_content.toLowerCase();
//				String new_keyword = term.replaceAll("\\s", "-");
//				String new_keyword2 = term.replaceAll("\\s", "");
				
				String new_keyword = StringUtils.replace(term, "\\s", "-");
				String new_keyword2 = StringUtils.replace(term, "\\s", "");
				
				if(entry_content.contains(term.toLowerCase()) || 
						entry_content.contains(new_keyword.toLowerCase()) || 
						entry_content.contains(new_keyword2.toLowerCase())) {
					return false;
	            }
    		}
    	}
    	
    	if(term_container.containsKey(or_exclude)) {
    		filter_terms = term_container.get(or_exclude);
    		int count = 0;
    		for (String term : filter_terms) {
//    			String new_term = term.replaceAll("\\s", "( |-|)");
//    	        Pattern match_pattern = Pattern.compile("(?iu)(^|\\b|\\s)(" + new_term + ")(\\b|$)");
//    	        Matcher matcher = match_pattern.matcher(entry_content);
//    	        if (!matcher.find()) {
//    	        	count++;
//    	        }
    	        
    	        entry_content = entry_content.toLowerCase();
//				String new_keyword = term.replaceAll("\\s", "-");
//				String new_keyword2 = term.replaceAll("\\s", "");
    	        String new_keyword = StringUtils.replace(term, "\\s", "-");
				String new_keyword2 = StringUtils.replace(term, "\\s", "");
				if(!entry_content.contains(term.toLowerCase()) && 
						!entry_content.contains(new_keyword.toLowerCase()) && 
						!entry_content.contains(new_keyword2.toLowerCase())) {
					count++;
	            }
    		}
    		
    		if(count == 0) {
    			return false;
    		}
    	}
    	
    	if(term_container.containsKey(or_include)) {
    		filter_terms = term_container.get(or_include);
    		int count = 0;
    		for (String term : filter_terms) {
//    			String new_term = term.replaceAll("\\s", "( |-|)");
//    	        Pattern match_pattern = Pattern.compile("(?iu)(^|\\b|\\s)(" + new_term + ")(\\b|$)");
//    	        Matcher matcher = match_pattern.matcher(entry_content);
//    	        if (matcher.find()) {
//    	        	count++;
//    	        }
    	        
    	        entry_content = entry_content.toLowerCase();
//				String new_keyword = term.replaceAll("\\s", "-");
//				String new_keyword2 = term.replaceAll("\\s", "");
				String new_keyword = StringUtils.replace(term, "\\s", "-");
				String new_keyword2 = StringUtils.replace(term, "\\s", "");
				if(entry_content.contains(term.toLowerCase()) || 
						entry_content.contains(new_keyword.toLowerCase()) || 
						entry_content.contains(new_keyword2.toLowerCase())) {
					count++;
	            }
    		}
    		
    		if(count == 0) {
    			return false;
    		}
    	}
    	
    	return true;
    }
    
    /*
     * order = 0, get minimum
     * order = 1, get maximum
     */
    public float get_max(List<String> terms, int order) {
    	if(terms.size() <= 1) {
    		return Float.parseFloat(terms.get(0));
    	}
    	
    	float res;
    	float tmp;
    	if(order == 0) {
    		res = 1000f;
    	} else {
    		res = -1000f;
    	}
    	
    	for(String term : terms) {
			tmp = Float.parseFloat(term);
			int diff = Float.compare(tmp, res);
			if(order == 0) {
				if(diff < 0) {
					res = tmp;
				}
			} else {
				if(diff > 0) {
					res = tmp;
				}
			}
    	}
    	return res;
    }
    
    public boolean check_by_score(float entry_score, Map<String, List<String>>term_container) {
    	String and_et = "1:0";
    	String and_gte = "1:2";
    	String and_lte = "1:-2";
    	String or_et = "0:0";
    	String or_gte = "0:2";
    	String or_lte = "0:-2";
    	
    	float tmp;
    	if(term_container.containsKey(and_gte)) {
    		tmp = get_max(term_container.get(and_gte), 1);
    		int diff = Float.compare(entry_score, tmp);
    		if (diff < 0) {
    			return false;
    		}		
    	}
    	
    	if(term_container.containsKey(and_lte)) {
    		tmp = get_max(term_container.get(and_lte), 0);
    		int diff = Float.compare(entry_score, tmp);
    		if (diff > 0) {
    			return false;
    		}		
    	}
    	
    	if(term_container.containsKey(and_et)) {
    		tmp = get_max(term_container.get(and_et), 0);
    		int diff = Float.compare(entry_score, tmp);
    		if (diff != 0) {
    			return false;
    		}		
    	}
    	
    	if(term_container.containsKey(or_gte)) {
    		tmp = get_max(term_container.get(or_gte), 0);
    		int diff = Float.compare(entry_score, tmp);
    		if (diff >= 0) {
    			return true;
    		}		
    	}
    	
    	if(term_container.containsKey(or_lte)) {
    		tmp = get_max(term_container.get(or_lte), 1);
    		int diff = Float.compare(entry_score, tmp);
    		if (diff <= 0) {
    			return true;
    		}		
    	}
    	
    	if(term_container.containsKey(or_et)) {
    		tmp = get_max(term_container.get(or_et), 0);
    		int diff = Float.compare(entry_score, tmp);
    		if (diff != 0) {
    			return true;
    		}		
    	}
    	
    	return true;
    }
    
    /*
     * pos: 1, neu: 0, neg: -1
     */
    public boolean check_by_sentiment(String entry_senti, Map<String, List<String>>term_container) {
    	
    	if(term_container.containsKey("1")) {
    		List<String> senti_and = term_container.get("1");
    		if(senti_and.contains(entry_senti)) {
    			return true;
    		}
    	}
    	
    	if(term_container.containsKey("0")) {
    		List<String> senti_or = term_container.get("0");
    		if(senti_or.contains(entry_senti)) {
    			return true;
    		}
    	}
    	
    	return false;
    }
    
    @SuppressWarnings("unchecked")
	public boolean filter_check(Map<String, Object> entry, Map<String, Map<String, List<String>>> subject_container) {
    	
    	String entry_type = (String)entry.get("item_type");
    	
    	if(subject_container.containsKey("term")) {
    		
    		List<String> terms_array = (ArrayList<String>)entry.get("tags_array");
    		if( !check_by_term(terms_array, subject_container.get("term")) ) {
    			return false;
    		}
    	}
    	
    	if(subject_container.containsKey("score")) {
    		
    		float entry_score = Float.parseFloat((String) entry.get("score"));
    		if( !check_by_score(entry_score, subject_container.get("score")) ) {
    			return false;
    		}
    	}
    	
    	if ("Tweet".equals(entry_type)) {
    		
        	if(subject_container.containsKey("content")) {
        		
        		String entry_content = (String)entry.get("tweet"); 
        		if( !check_by_content(entry_content, subject_container.get("content")) ) {
        			return false;
        		}
        	}
        	
        	
    		if(subject_container.containsKey("follower")) {
    			float entry_score = Float.parseFloat((String) entry.get("followers"));
    			if( !check_by_score(entry_score, subject_container.get("follower")) ) {
    				return false;
    			}
        	}
        	
        	if(subject_container.containsKey("following")) {
        		float entry_score = Float.parseFloat((String) entry.get("following"));
    			if( !check_by_score(entry_score, subject_container.get("following")) ) {
    				return false;
    			}
        	}
        	
        	if(subject_container.containsKey("sentiment")) {
        		String entry_senti = String.valueOf(entry.get("sentiment"));
        		if( !check_by_sentiment(entry_senti, subject_container.get("sentiment")) ) {
        			return false;
        		}
        	}
        	
    	} else {
    		// blog post or sth.
    		if(subject_container.containsKey("content")) {
        		String entry_content = (String)entry.get("source"); 
        		if( !check_by_content(entry_content, subject_container.get("content")) ) {
        			return false;
        		}
        	}
    	}
    	
    	return true;
    }
    
    @SuppressWarnings("unchecked")
	public Map<String, Object> apply_actions(Map<String, Object> entry, int filter_id) throws SQLException {
    	
    	Connection con = null;
    	PreparedStatement action_st = null;
    	ResultSet filter_actions = null;
    	
    	try {
    		con = this.cp_pool.getConnection();
	    	action_st = con.prepareStatement("SELECT `filter_actions`.* FROM `filter_actions` WHERE `filter_actions`.`filter_id` = " + filter_id);
	    	filter_actions = action_st.executeQuery();
	    	String action_type_name_str;
	    	String action_name;
	    	String action_value;
	    	while(filter_actions.next()) {
	    		action_type_name_str = ACTION_TYPE[filter_actions.getInt("action_type")];
	    		action_name = filter_actions.getString("action");
	    		
	    		if(action_name == null) {
	    			continue;
	    		}
	    		
	    		if(action_type_name_str.equals("Add to list")) {
	    			if(action_name.equals("New ...")) {
	    				
	    				action_value = filter_actions.getString("action_value");
	    				List<String> entry_lists = (ArrayList<String>) entry.get("lists_array");
	    				if(action_value != null) {
	    					if(entry_lists == null) {
	    						entry_lists = new ArrayList<String>();
	    						entry_lists.add(action_value);
	    					} else if (!entry_lists.contains(action_value)) {
	    						entry_lists.add(action_value);
	    					}
	    					entry.put("lists_array", entry_lists);
	    				}
	    				
	    			} else {
	    				List<String> entry_lists = (ArrayList<String>) entry.get("lists_array");
	    				if(action_name != null) {
	    					if(entry_lists == null) {
	    						entry_lists = new ArrayList<String>();
	    						entry_lists.add(action_name);
	    					} else if (!entry_lists.contains(action_name)) {
	    						entry_lists.add(action_name);
	    					}
	    					entry.put("lists_array", entry_lists);
	    				}
	    			}
	    		}
	    		
	    		if(action_type_name_str.equals("Modify score")) {
	    			action_value = filter_actions.getString("action_value");
	    			float entry_score = Float.parseFloat((String) entry.get("score"));
	    			float action_value_f = Float.parseFloat(action_value);
	    			
	    			if(action_name.equals("multiply by")) {
	    				entry_score = entry_score * action_value_f;
	    				entry.put("score", Float.toString(entry_score));
	    			}
	    			
	    			else if(action_name.equals("divide by") && (Float.compare(action_value_f, 0) != 0) ) {
	    				entry_score = entry_score / action_value_f;
	    				entry.put("score", Float.toString(entry_score));
	    			}
	    			
	    			else if(action_name.equals("add by")) {
	    				entry_score = entry_score + action_value_f;
	    				entry.put("score", Float.toString(entry_score));
	    			}
	    			
	    			else if(action_name.equals("subtract by")) {
	    				entry_score = entry_score - action_value_f;
	    				entry.put("score", Float.toString(entry_score));
	    			}
	    			
	    			// if(entry_score > 100f)
	    		}	
	    	}
	    	return entry;
			
		} catch (SQLException e) {
			LOG.error("Failed to get filter from MySQL for id: " + String.valueOf(filter_id));
			LOG.error(e.getMessage(), e);
			return null;
			
		} finally {
			if (filter_actions != null) try { filter_actions.close(); } catch (SQLException quiet) {}
			if (action_st != null) try { action_st.close(); } catch (SQLException quiet) {}
			if (con != null) try { con.close(); } catch (SQLException quiet) {}
		}
    }
    
    @SuppressWarnings("unchecked")
	public Map<String, Object> apply_actions_batch(Map<String, Object> entry, String filter_ids) throws SQLException {
    	
    	Connection con = null;
    	PreparedStatement action_st = null;
    	ResultSet filter_actions = null;
    	
    	try {
    		con = this.cp_pool.getConnection();
	    	action_st = con.prepareStatement("SELECT `filter_actions`.* FROM `filter_actions` WHERE `filter_actions`.`filter_id` IN (" + filter_ids + ")");
	    	filter_actions = action_st.executeQuery();
	    	String action_type_name_str;
	    	String action_name;
	    	String action_value;
	    	while(filter_actions.next()) {
	    		action_type_name_str = ACTION_TYPE[filter_actions.getInt("action_type")];
	    		action_name = filter_actions.getString("action");
	    		
	    		if(action_name == null) {
	    			continue;
	    		}
	    		
	    		if(action_type_name_str.equals("Add to list")) {
	    			if(action_name.equals("New ...")) {
	    				
	    				action_value = filter_actions.getString("action_value");
	    				List<String> entry_lists = (ArrayList<String>) entry.get("lists_array");
	    				if(action_value != null) {
	    					if(entry_lists == null) {
	    						entry_lists = new ArrayList<String>();
	    						entry_lists.add(action_value);
	    					} else if (!entry_lists.contains(action_value)) {
	    						entry_lists.add(action_value);
	    					}
	    					entry.put("lists_array", entry_lists);
	    				}
	    				
	    			} else {
	    				List<String> entry_lists = (ArrayList<String>) entry.get("lists_array");
	    				if(action_name != null) {
	    					if(entry_lists == null) {
	    						entry_lists = new ArrayList<String>();
	    						entry_lists.add(action_name);
	    					} else if (!entry_lists.contains(action_name)) {
	    						entry_lists.add(action_name);
	    					}
	    					entry.put("lists_array", entry_lists);
	    				}
	    			}
	    		}
	    		
	    		if(action_type_name_str.equals("Modify score")) {
	    			action_value = filter_actions.getString("action_value");
	    			float entry_score = Float.parseFloat((String) entry.get("score"));
	    			float action_value_f = Float.parseFloat(action_value);
	    			
	    			if(action_name.equals("multiply by")) {
	    				entry_score = entry_score * action_value_f;
	    				entry.put("score", Float.toString(entry_score));
	    			}
	    			
	    			else if(action_name.equals("divide by") && (Float.compare(action_value_f, 0) != 0) ) {
	    				entry_score = entry_score / action_value_f;
	    				entry.put("score", Float.toString(entry_score));
	    			}
	    			
	    			else if(action_name.equals("add by")) {
	    				entry_score = entry_score + action_value_f;
	    				entry.put("score", Float.toString(entry_score));
	    			}
	    			
	    			else if(action_name.equals("subtract by")) {
	    				entry_score = entry_score - action_value_f;
	    				entry.put("score", Float.toString(entry_score));
	    			}
	    			
	    			// if(entry_score > 100f)
	    		}	
	    	}

	    	return entry;
    	} catch (SQLException e) {
			LOG.error("Failed to get batch filters information from MySQL");
			LOG.error(e.getMessage(), e);
			return null;
			
		} finally {
			if (filter_actions != null) try { filter_actions.close(); } catch (SQLException quiet) {}
			if (action_st != null) try { action_st.close(); } catch (SQLException quiet) {}
			if (con != null) try { con.close(); } catch (SQLException quiet) {}
		}
    }
    
    public Map<String, Object> filter_check_by_project(Map<String, Object> entry, int project_id) throws SQLException {
    	Map<String, Map<String, List<String>>> subject_container;
    	String filter_key;
    	String subject_type;
    	String filter_ids;
    	Set<String> qulified_filters = new HashSet<String>();
    	
    	Map<String, Map<String, Map<String, List<String>>>> filter_container = filterCache.get(project_id);
    	
    	if(filter_container == null) {
    		filter_container = get_filters(project_id);
    		if(filter_container != null) {
    			filterCache.put(project_id, filter_container, secondsToLive);
    		}
    	}
    	
    	String entry_type = (String)entry.get("item_type");
    	
    	for (Map.Entry<String, Map<String, Map<String, List<String>>>> subjects : filter_container.entrySet()) {
    		filter_key = subjects.getKey();
    		subject_container = subjects.getValue();
    		String[] filter_key_arr = filter_key.split(":");
    		subject_type = filter_key_arr[1];
    		
    		// Notice that each entry type will actually match only one type of filter subject ( either specific one like 'Tweet', or 'All')
    		// Although filter has many subjects.
    		if ("All".equals(subject_type) || entry_type.equals(subject_type)) {
    			if (filter_check(entry, subject_container)) {
    				
    				// add the id of matched filter into set.
    				qulified_filters.add(String.valueOf(filter_key_arr[0]));
    				
//    				int filter_id = Integer.parseInt(filter_key_arr[0]);
//    				entry = apply_actions(entry, filter_id);
    			}
    		}
    	}
    	
    	if(!qulified_filters.isEmpty()) {
    		if(qulified_filters.size() > 1) {
    			String[] fids = qulified_filters.toArray(new String[qulified_filters.size()]);
    			filter_ids = StringUtils.join(fids, ",");
    			entry = apply_actions_batch(entry, filter_ids);
    		} else {
    			for(String id_str : qulified_filters) {
    				int filter_id = Integer.parseInt(id_str);
    				entry = apply_actions(entry, filter_id);
    			}
    		}
    	}
    	return entry;
    }
    
//    public static void main(String[] args) throws Exception {
//    	
//    }
}
