package com.datadio.storm.lib;

import java.util.List;
import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class DioSQL {
    
//	private final String url;
//	private final String database;
//	private final String user;
//	private final String password;
//	private BoneCP cp_pool = null;
	private ComboPooledDataSource cp_pool = null;
	
//	private Connection con = null;
//    private Statement st = null;
//    private ResultSet rs = null;
	
	private final String client_query = "SELECT name,url FROM `clients` WHERE `project_id` = ?";
    private final String competitor_query = "SELECT name,url FROM `competitors` WHERE `project_id` = ?";
    private final String terms_query = "SELECT project_id,keyword FROM `terms` WHERE `twitter` = 1";
    private final String blog_terms_query = "SELECT project_id,keyword FROM `terms` WHERE `blogs` = 1";
    private final String forum_terms_query = "SELECT project_id,keyword FROM `terms` WHERE `forums` = 1";
    private final String company_query = "SELECT company_id FROM `projects` WHERE `projects`.`id` = ? LIMIT 1";
    
    public static final int TWITTER = 0;
    public static final int BLOG = 1;
    public static final int FORUM = 2;
    
    private static final Logger LOG = LoggerFactory.getLogger(DioSQL.class);
    
    public DioSQL() {
    	this("jdbc:mysql://localhost:3306", "datadio", "datadio", "j38h78237fg32");
    }
    
    public DioSQL(String url, String database, String user, String password) {
//    	this.url = url;
//    	this.database = database;
//    	this.user = user;
//    	this.password = password;
    	
    	// use BoneCP pool
    	// will throw Unable to start JMX, javax.management.InstanceAlreadyExistsException
    	// if do not set pool name
    	
//    	try {
////    	    System.out.println("Loading driver...");
////    	    Class.forName("com.mysql.jdbc.Driver");
//    	    Class.forName("org.mariadb.jdbc.Driver");
////    	    System.out.println("Driver loaded!");
//    	} catch (ClassNotFoundException e) {
//    	    throw new RuntimeException("Cannot find the driver in the classpath!", e);
//    	}
//    	
//    	String rndchars=RandomStringUtils.randomAlphanumeric(5);
//    	
//    	BoneCPConfig cpConfig = new BoneCPConfig();
//    	cpConfig.setPoolName(rndchars);
//    	cpConfig.setJdbcUrl(url + "/" + database);
//    	cpConfig.setUser(user);
//    	cpConfig.setPassword(password);
//    	cpConfig.setMaxConnectionAgeInSeconds(240);
//    	cpConfig.setDefaultAutoCommit(true);
//    	
//    	try {
//			cp_pool = new BoneCP(cpConfig);
//		} catch (SQLException e) {
//			LOG.error("Couldn't connect to SQL. Fail. " + e);
//			
//		}
    	
    	// use c3p0 pool
    	cp_pool = new ComboPooledDataSource();
    	try {
//			cp_pool.setDriverClass( "com.mysql.jdbc.Driver" );
    		cp_pool.setDriverClass( "org.mariadb.jdbc.Driver" );
		} catch (PropertyVetoException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} //loads the jdbc driver            
    	cp_pool.setJdbcUrl( url + "/" + database );
    	cp_pool.setUser(user);                                  
    	cp_pool.setPassword(password);                                  
    	cp_pool.setMaxStatements(300);
    	cp_pool.setMaxConnectionAge(240);
    	
    	// use normal way
//        try {
//            this.con = DriverManager.getConnection(this.url + "/" + this.database, this.user, this.password);
//        } catch(SQLException e) {
//            System.out.println("Couldn't connect to SQL. Fail. " + e);
//        }
    }
    
    /**
     * 
     * @return null if has SQLException
     */
    public Map<String, List<String>> getClients(int pid) {
    	
    	Connection con = null;
    	PreparedStatement st = null;
    	ResultSet rs = null;
    	
    	try {
    		con = this.cp_pool.getConnection();
    		
        	st = con.prepareStatement(client_query);
        	st.setString(1, Integer.toString(pid));
        	rs = st.executeQuery();
            
            List<String> names = new ArrayList<String>();
            List<String> links = new ArrayList<String>();
            
            while(rs.next()) {
                names.add( rs.getString("name") );
                links.add( rs.getString("url") );
            }
            
            Map<String, List<String>> this_container = new HashMap<String, List<String>>();
            
            this_container.put("names", names);
            this_container.put("urls", links);

            return this_container;
			
		} catch (SQLException e) {
			LOG.error("Failed to get Clients information from MySQL for project: " + String.valueOf(pid));
			LOG.error(e.getMessage(), e);
			return null;
			
		} finally {
			if (rs != null) try { rs.close(); } catch (SQLException quiet) {}
			if (st != null) try { st.close(); } catch (SQLException quiet) {}
			if (con != null) try { con.close(); } catch (SQLException quiet) {}
		}
        
    }
    
    /**
     * 
     * @return null if has SQLException
     */
    public Map<String, List<String>> getCompetitors(int pid){
    	
    	Connection con = null;
    	PreparedStatement st = null;
    	ResultSet rs = null;
    	
    	try {
    		con = this.cp_pool.getConnection();
    		st = con.prepareStatement(competitor_query);
    		st.setString(1, Integer.toString(pid));
    		rs = st.executeQuery();
    		
    		List<String> names = new ArrayList<String>();
    	    List<String> links = new ArrayList<String>();
    	        
	        while(rs.next()) {
	            names.add( rs.getString("name") );
	            links.add( rs.getString("url") );
	        }
	        
	        Map<String, List<String>> this_container = new HashMap<String, List<String>>();
	        
	        this_container.put("names", names);
	        this_container.put("urls", links);
	        
	        return this_container;
    		
		} catch (SQLException e) {
			LOG.error("Failed to get Competitors from MySQL for project: " + String.valueOf(pid));
			LOG.error(e.getMessage(), e);
			return null;
			
		} finally {
			if (rs != null) try { rs.close(); } catch (SQLException quiet) {}
			if (st != null) try { st.close(); } catch (SQLException quiet) {}
			if (con != null) try { con.close(); } catch (SQLException quiet) {}
		}   
    }
    
    /**
     * 
     * @return null if has SQLException
     */
    public Map<Integer, List<String>> getKeywords(int TYPE) {
    	String query_str = null;
    	switch (TYPE) {
		case DioSQL.TWITTER: query_str = terms_query;
			break;
		case DioSQL.BLOG: query_str = blog_terms_query;
			break;
		case DioSQL.FORUM: query_str = forum_terms_query;
			break;
		default: query_str = terms_query;
			break;
		}
    	
    	Connection con = null;
    	PreparedStatement st = null;
    	ResultSet rs = null;
    	
    	try {
    	
    		con = this.cp_pool.getConnection();
    		st = con.prepareStatement(query_str);
    		rs = st.executeQuery();
        
	        Map<Integer, List<String>> term_container = new HashMap<Integer, List<String>>();
	        Integer pid;
	        String kwd;
	        List<String> kwds;
        
	        while(rs.next()) {
	            pid = rs.getInt("project_id");
	            kwd = rs.getString("keyword");
	            
	            if(term_container.get(pid) != null) {
	            	kwds = term_container.get(pid);
	            	kwds.add(kwd);
	            	term_container.put(pid,kwds);
	            } else {
	            	kwds = new ArrayList<String>();
	            	kwds.add(kwd);
	            	term_container.put(pid, kwds);
	            }
	        }
	        return term_container;
	        
    	} catch (SQLException e) {
			LOG.error("Failed to get Keywords from MySQL");
			LOG.error(e.getMessage(), e);
			return null;
			
		} finally {
			if (rs != null) try { rs.close(); } catch (SQLException quiet) {}
			if (st != null) try { st.close(); } catch (SQLException quiet) {}
			if (con != null) try { con.close(); } catch (SQLException quiet) {}
		}   
        
    }
    
    /**
     * 
     * @return null if has SQLException
     */
    public Map<Integer, List<String>> getKeywordsForTwitter() {
    	
    	Connection con = null;
    	PreparedStatement st = null;
    	ResultSet rs = null;
    	
    	try {
    		con = this.cp_pool.getConnection();
    		st = con.prepareStatement(terms_query);
    		rs = st.executeQuery();
        
	        Map<Integer, List<String>> term_container = new HashMap<Integer, List<String>>();
	        Integer pid;
	        String kwd;
	        List<String> kwds;
	        
	        while(rs.next()) {
	            pid = rs.getInt("project_id");
	            kwd = rs.getString("keyword");
	            
	            if(term_container.get(pid) != null) {
	            	kwds = term_container.get(pid);
	            	kwds.add(kwd);
	            	term_container.put(pid,kwds);
	            } else {
	            	kwds = new ArrayList<String>();
	            	kwds.add(kwd);
	            	term_container.put(pid, kwds);
	            }
	        }
	        return term_container;
	       
    	} catch (SQLException e) {
			LOG.error("Failed to get Keywords for Twitter");
			LOG.error(e.getMessage(), e);
			return null;
			
		} finally {
			if (rs != null) try { rs.close(); } catch (SQLException quiet) {}
			if (st != null) try { st.close(); } catch (SQLException quiet) {}
			if (con != null) try { con.close(); } catch (SQLException quiet) {}
		}   
        
    }
    
    /**
     * Get company id according to project id
     * @param project_id
     * @return -1 if company is not found
     * @throws SQLException
     */
    public int get_company_id(int project_id) {
    	
    	Connection con = null;
    	PreparedStatement st = null;
    	ResultSet rs = null;
    	
    	try {
    		con = this.cp_pool.getConnection();
    		st = con.prepareStatement(company_query);
    		st.setString(1, Integer.toString(project_id));
    		rs = st.executeQuery();
	    	int company_id = -1;
	    	while(rs.next()) {
	    		company_id = rs.getInt("company_id");
	    	}
	    	return company_id;
	    	
    	} catch (SQLException e) {
			LOG.error("Failed to get Keywords for Twitter");
			LOG.error(e.getMessage(), e);
			return -1;
			
		} finally {
			if (rs != null) try { rs.close(); } catch (SQLException quiet) {}
			if (st != null) try { st.close(); } catch (SQLException quiet) {}
			if (con != null) try { con.close(); } catch (SQLException quiet) {}
		}  
	    	
    }
//    // test sample
//    public static void main(String[] args) throws Exception {
//    	DioSQL test = new DioSQL("jdbc:mysql://localhost:3306", "socialmon_develop_test", "root", "abc123");
//    	Integer pid = new Integer(3);
//    	
//    	Map<String, List<String>> clients = test.getClients(pid);
//    	System.out.println("Clients for" + pid.toString());
//    	System.out.println("names");
//    	for(String name : clients.get("names")){
//    		System.out.println(name);
//    	}
//    	
//    	System.out.println("urls");
//    	for(String url : clients.get("urls")){
//    		System.out.println(url);
//    	}
//    	
//    	Map<Integer, List<String>> term_container = test.getKeywordsForTwitter();
//    	
//    	for(Integer key : term_container.keySet()) {
//    		System.out.println("Twitter tracked keywords for project" + key.toString());
//    		
//    		for(String keyword : term_container.get(key)){
//        		System.out.println(keyword);
//        	}
//    		
//    		System.out.println();
//    	}
//    	
//    }
    
}
