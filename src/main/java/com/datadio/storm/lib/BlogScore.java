package com.datadio.storm.lib;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sun.syndication.feed.synd.SyndCategoryImpl;
import com.sun.syndication.feed.synd.SyndEntry;

import static org.apache.commons.lang.StringEscapeUtils.unescapeHtml;

/**
 * 
 * This is the worker that will return the score for a contextual document. 
 * 
 * It needs to be able to support taking in multiple keywords for a single project at a time.
 * This way we can score if someone has 5/7 keywords found for a single project on a single page.
 * 
 */
public class BlogScore {
    
    public static Integer GetScore(Integer clients, Integer competitors, Integer keywords) {
        // Actually handling the scoring in here.
        
        competitors = (int) Math.ceil((competitors * 0.25));
        keywords = (int) Math.ceil((keywords * 0.50));
        
        return (clients + competitors + keywords);
    }
    
    public static Integer ScoreItem(Map<String, List<String>> clients, Map<String, List<String>> competitors, 
    		WebPage article, SyndEntry entry, String page_body, Integer tagged_keywords_count, Integer total_keyword_count) {
       
        // parseFlag - maybe?
        // If 0, they're competitors
        // If 1, they're clients
    	
    	List<String> client_names = null;
    	List<String> client_links = null;
    	
    	List<String> competitor_names = null;
        List<String> competitor_links = null;
        
        if(clients != null) {
        	client_names = clients.get("name");
        	client_links = clients.get("url");
        }
        
        if(competitors != null) {
        	competitor_names = competitors.get("name");
        	competitor_links = competitors.get("url");
        }

        Integer names_count = 0;
        Integer links_count = 0;

        Integer client_names_count = (client_names == null ? 0 : client_names.size());
        Integer client_links_count = (client_links == null ? 0 : client_links.size());
        
        Integer competitor_names_count = (competitor_names == null ? 0 : competitor_names.size());
        Integer competitor_links_count = (competitor_links == null ? 0 : competitor_links.size());
        
        // keyword_count should already be set since we compare everything in this function
        
        Integer title_score     = 0;
        Integer url_score       = 0;
        Integer body_score      = 0;
        Integer tag_score       = 0;
        Integer meta_kw_score   = 0;
        Integer meta_desc_score = 0;
        
        String page_title = article.getTitle() == null ? "" : article.getTitle().toLowerCase();
        String page_URL = unescapeHtml(article.getUrl()).toLowerCase();
        // Body is already set as page_body
        String body_lower = page_body.toLowerCase();
        
        // First we'll check the title.
        if(client_names != null && !client_names.isEmpty()) {
	        for (Iterator<String> it = client_names.iterator(); it.hasNext();) {
	            String current = (String) it.next();
	            
	            if(current == null) {
	                    continue;
	            }
	            
	            if(CheckWithVariations(page_title, current) == true) {
	                title_score = 25;
	                break;
	            }
	            
	        }
        }
        
        if(title_score != 25) {
        	
        	if(client_links != null && !client_links.isEmpty()) {
	            for (Iterator<String> it = client_links.iterator(); it.hasNext();) {
	                String current = (String) it.next();
	                if(current == null) {
	                    continue;
	                }
	
	                if(CheckWithVariations(page_title, current) == true) {
	                    title_score = 25;
	                    break;
	                }
	
	            }
        	}
            
            Integer title_count = 0;
            
            if(competitor_names != null && !competitor_names.isEmpty()) {
	            for (Iterator<String> it = competitor_names.iterator(); it.hasNext();) {
	                String current = (String) it.next();
	                if(current == null) {
	                    continue;
	                }
	
	                if(CheckWithVariations(page_title, current) == true) {
	                    title_count++;
	                }
	
	            }
            }
            
            if(competitor_links != null && !competitor_links.isEmpty()) {
	            for (Iterator<String> it = competitor_links.iterator(); it.hasNext();) {
	                String current = (String) it.next();
	                if(current == null) {
	                    continue;
	                }
	
	                if(CheckWithVariations(page_title, current) == true) {
	                    title_count++;
	                }
	
	            }
            }
            
            if (competitor_names_count + competitor_links_count > 0 ) {
            	title_score = (int) Math.ceil(((double)title_count / (competitor_names_count + competitor_links_count)) * 25);
            } else {
            	title_score = 0;
            }
            
        }      
        
        //System.out.prinln("Title Score -> " + title_score);
        
        // Then we'll check the URL
        if(client_names != null && !client_names.isEmpty()) {
	        for (Iterator<String> it = client_names.iterator(); it.hasNext();) {
	            String current = (String) it.next();
	            if(current == null) {
	                    continue;
	            }
	            
	            if(CheckWithVariations(page_URL, current) == true) {
	                url_score = 25;
	                break;
	            }
	            
	        }
        }
        
        if(url_score != 25) {
        	if(client_links != null && !client_links.isEmpty()) {
	            for (Iterator<String> it = client_links.iterator(); it.hasNext();) {
	                String current = (String) it.next();
	                if(current == null) {
	                    continue;
	                }
	
	                if(CheckWithVariations(page_URL, current) == true) {
	                    url_score = 25;
	                    break;
	                }
	
	            }
        	}
            
            Integer url_count = 0;
            if(competitor_names != null && !competitor_names.isEmpty()) {
	            for (Iterator<String> it = competitor_names.iterator(); it.hasNext();) {
	                String current = (String) it.next();
	                if(current == null) {
	                    continue;
	                }
	
	                if(CheckWithVariations(page_URL, current) == true) {
	                    url_count++;
	                }
	
	            }
            }
            
            if(competitor_links != null && !competitor_links.isEmpty()) {
	            for (Iterator<String> it = competitor_links.iterator(); it.hasNext();) {
	                String current = (String) it.next();
	                if(current == null) {
	                    continue;
	                }
	
	                if(CheckWithVariations(page_URL, current) == true) {
	                    url_count++;
	                }
	
	            }
            }
            
            if(competitor_names_count + competitor_links_count > 0 ) {
            	url_score = (int) Math.ceil(((double)url_count / (competitor_names_count + competitor_links_count)) * 25);
            } else {
            	url_score = 0;
            }
        
        }
        
        //System.out.prinln("URL Score -> " + url_score);
        
        // Then we'll check the body
        // ...didn't we already check the body before when parsing to figure
        //    out how many keywords from a project are in it...?
        // 
        // If we do have to check, use CheckWithoutVariations(haystack, needle)
        // This is for clients/competitors, though. Not keywords... Duh.
        
        Integer body_count = 0;
        
        if(client_names_count > 0) {
            for (Iterator<String> it = client_names.iterator(); it.hasNext();) {
                String current = (String) it.next();
                if(current == null) {
                        continue;
                }

                if(CheckWithVariations(body_lower, current) == true) {
                    body_count++;
                }

            }
        }
        
        if(client_links_count > 0) {
        
            for (Iterator<String> it = client_links.iterator(); it.hasNext();) {
                String current = (String) it.next();
                if(current == null) {
                    continue;
                }

                if(CheckWithoutVariations(body_lower, current) == true) {
                    body_count++;
                }

            }
        
        }
        
        if(competitor_names_count > 0) {
            for (Iterator<String> it = competitor_names.iterator(); it.hasNext();) {
                String current = (String) it.next();
                if(current == null) {
                        continue;
                }

                if(CheckWithVariations(body_lower, current) == true) {
                    body_count++;
                }

            }
        }
        
        if(competitor_links_count > 0) {
        
            for (Iterator<String> it = competitor_links.iterator(); it.hasNext();) {
                String current = (String) it.next();
                if(current == null) {
                    continue;
                }

                if(CheckWithoutVariations(body_lower, current) == true) {
                    body_count++;
                }

            }
        
        }
        
        
//        Integer kw_count = 0;
//        // Find the keywords for the score...
//        if(keywords.size() > 0) {
//            
//            for (Iterator<String> it = keywords.iterator(); it.hasNext();) {
//                String current = (String) it.next();
//                if(current == null) {
//                    continue;
//                }
//
//                if(CheckWithoutVariations(body_lower, current.toLowerCase()) == true) {
//                    kw_count++;
//                }
//
//            }
//        
//        }
        
        if(client_names_count < 0 && 
        	client_links_count < 0 && 
        	competitor_names_count < 0 && 
        	competitor_links_count < 0) {
            //System.out.prinln("No Names & No Links -- Body Score: 25");
            body_score = 25;            
        } else {
            body_score = (int) Math.ceil(((double)body_count / (client_names_count + client_links_count + competitor_names_count + competitor_links_count)) * 15);
            body_score += (int) Math.ceil(((double)tagged_keywords_count / total_keyword_count) * 20);
            //System.out.prinln("Body Score => " + body_score);
        }
        
        
        if(entry != null && entry.getCategories().size() > 0 && client_names_count > 0) {
            
        	List categories = entry.getCategories();
            // Then we'll check the tags
            
            List tmpClientsLower = new ArrayList();
            
                for (Iterator it = client_names.iterator(); it.hasNext();) {
                String current = (String) it.next();

                if(current == null) {
                    continue;
                }
                
                tmpClientsLower.add(current.toLowerCase());
            }
                
            List tmpCompetitorsLower = new ArrayList();
            
                for (Iterator it = competitor_names.iterator(); it.hasNext();) {
                String current = (String) it.next();

                if(current == null) {
                    continue;
                }
                
                tmpCompetitorsLower.add(current.toLowerCase());
            }
            
            Set<String> tmpClients = new HashSet<String>(tmpClientsLower);
            Set<String> tmpCompetitors = new HashSet<String>(tmpCompetitorsLower);
            
            Integer tagCount = 0;

            for(Iterator it = categories.iterator(); it.hasNext();) {
                SyndCategoryImpl a = (SyndCategoryImpl) it.next();
                if(CheckSet(tmpClients, a.getName().toLowerCase()) == true) {
                    tag_score = 5;
                    break;
                }
            }
            
            for(Iterator it = categories.iterator(); it.hasNext();) {
                SyndCategoryImpl a = (SyndCategoryImpl) it.next();
                if(CheckSet(tmpCompetitors, a.getName().toLowerCase()) == true) {
                    tag_score = 5;
                }
            }
            
            //System.out.prinln("We found " + tagCount + "/" + (tmpCompetitors.size()) + " in the tags.");

            if((tmpClients.size() > 0 || tmpCompetitors.size() > 0) && tag_score != 5) {
                tag_score = (int) Math.ceil(((double)tagCount / tmpCompetitors.size()) * 5);
                //System.out.prinln("Tag Score => " + tag_score);
            } else {
                //System.out.prinln("Tag Score => " + tag_score);
                tag_score = 5;
            }
            
            // Clean it up a little, just in case.
            tmpClients = null;
            tmpCompetitors = null;
        
        } else {
            // If there are no tags ... full credit.
            tag_score = 5;
        }
        
        
        String meta_desc = article.getMetaDescription();
        
        if(meta_desc != null && meta_desc.length() > 0 &&
        		client_names_count > 0 &&
        		client_links_count > 0 &&
        		competitor_names_count > 0 &&
        		competitor_links_count > 0) {
                    
            // Then we'll check the meta desc
        	if(client_names_count > 0) {
	            for (Iterator it = client_names.iterator(); it.hasNext();) {
	                String current = (String) it.next();
	
	                if(current == null) {
	                    continue;
	                }
	                
	                if(CheckWithVariations(meta_desc, current) == true) {
	                    //System.out.prinln("** -> Found in Meta Desc: " + current);
	                    meta_desc_score = 5;
	                    break;
	                }
	
	            }
        	}

            if(meta_desc_score == 0) {
            	
            	if(client_links_count > 0) {
	                for (Iterator it = client_links.iterator(); it.hasNext();) {
	                    String current = (String) it.next();
	                    
	                    if(current == null) {
	                        continue;
	                    }
	                    
	                    if(CheckWithVariations(meta_desc, current) == true) {
	                        meta_desc_score = 5;
	                        break;
	                    }
	
	                }
            	}
                        
                if(meta_desc_score != 5) {
                
                    Integer meta_count = 0;
                    
                    if(competitor_names_count > 0) {
	                    for (Iterator it = competitor_names.iterator(); it.hasNext();) {
	                        String current = (String) it.next();
	
	                        if(current == null) {
	                            continue;
	                        }
	
	                        if(CheckWithVariations(meta_desc, current) == true) {
	                            meta_count++;
	                        }
	
	                    }
                    }
                    
                    if(competitor_links_count > 0) {
	                    for (Iterator it = competitor_links.iterator(); it.hasNext();) {
	                        String current = (String) it.next();
	
	                        if(current == null) {
	                            continue;
	                        }
	
	                        if(CheckWithVariations(meta_desc, current) == true) {
	                            meta_count++;
	                        }
	
	                    }
                    }
                    
                    if(competitor_names_count + competitor_links_count > 0) {
                    	meta_desc_score = (int) Math.ceil(((double)meta_count / (competitor_names_count + competitor_links_count)) * 5);
                    }else {
                    	meta_desc_score = 0;
                    }
                    
                }
            }   
        
        } else {
            // If there is no meta description, full credit.
            meta_desc_score = 5;
        }
        
        
        // Then we'll check the meta kws
        String meta_kw = article.getMetaKeywords();
        
        if(meta_kw != null && meta_kw.length() > 0) {
                    
            // Then we'll check the meta desc
        	if(client_names_count > 0) {
	            for (Iterator it = client_names.iterator(); it.hasNext();) {
	                String current = (String) it.next();
	
	                if(current == null) {
	                    continue;
	                }
	                
	                if(CheckWithVariations(meta_kw, current) == true) {
	                    //System.out.prinln("** -> Found in Meta Keywords: " + current);
	                    meta_kw_score = 5;
	                    break;
	                }
	
	            }
        	}

            if(meta_kw_score == 0) {
            	if(client_links_count > 0) {
	                for (Iterator it = client_links.iterator(); it.hasNext();) {
	                    String current = (String) it.next();
	
	                    if(current == null) {
	                        continue;
	                    }
	                    
	                    if(CheckWithVariations(meta_kw, current) == true) {
	                        meta_kw_score = 5;
	                        break;
	                    }
	
	                }
            	}

            }
            
            if(meta_kw_score != 5) {
                
                Integer meta_count = 0;
                if(competitor_names_count > 0) {
	                for (Iterator it = competitor_names.iterator(); it.hasNext();) {
	                    String current = (String) it.next();
	
	                    if(current == null) {
	                        continue;
	                    }
	                    
	                    if(CheckWithVariations(meta_kw, current) == true) {
	                        meta_count++;
	                    }
	
	                }
            	}
                
                if(competitor_links_count > 0) {
	                for (Iterator it = competitor_links.iterator(); it.hasNext();) {
	                    String current = (String) it.next();
	
	                    if(current == null) {
	                        continue;
	                    }
	                    
	                    if(CheckWithVariations(meta_kw, current) == true) {
	                        meta_count++;
	                    }
	
	                }
                }
                
                if(competitor_names_count + competitor_links_count > 0) {
                	meta_kw_score = (int) Math.ceil(((double)meta_count / (competitor_names_count + competitor_links_count)) * 5);
                }else {
                	meta_kw_score = 0;
                }
                
            }
        
        } else {
            // If there is no meta description, full credit.
            meta_kw_score = 5;
        }
        
        
        
        // By this point we've gotten all of the scores for the entity.
        
//        Integer title_score     = 0;
//        Integer url_score       = 0;
//        Integer body_score      = 0;
//        Integer tag_score       = 0;
//        Integer meta_kw_score   = 0;
//        Integer meta_desc_score = 0;
        
        return (title_score + url_score + body_score + tag_score + meta_desc_score + meta_kw_score);
    }
    
    private static Boolean CheckWithVariations(String haystack, String needle) {
        
        needle = needle.toLowerCase();
        
        if(haystack.indexOf(needle) != -1) {
            //System.out.prinln("Term " + needle + " Found in haystack.");
            return true;
        }
        
        if(needle.indexOf(" ") != -1) {
            return CheckVariations(haystack, needle);
        } // end if space is in needle
        
        return false;
    }
    
    private static Boolean CheckWithoutVariations(String haystack, String needle) {
        
        needle = needle.toLowerCase();
        
        if(haystack.indexOf(needle) != -1) {
            //System.out.prinln("Term " + needle + " Found in haystack.");
            return true;
        } else {
            return false;
        }
    }
    
    private static Boolean CheckSet(Set<String> haystack, String needle) {
        
        if(haystack.contains(needle)) {
            return true;
        } else {
            if(haystack.contains(needle.replaceAll(" ",""))) {
                return true;
            }
        }     
        
        return false;
    }
    
    private static Boolean CheckVariations(String haystack, String needle) {
        
            // There's a space in it. Let's make this work...
            
            // Check dashes
            if(haystack.indexOf(needle.replace(' ', '-')) != -1) {
                return true;
            }
            
            // Check underscores
            if(haystack.indexOf(needle.replace(' ', '_')) != -1) {
                return true;
            }
            
            // Check no spaces at all
            if(haystack.indexOf(needle.replaceAll(" ", "")) != -1) {
                return true;
            }
            
       // Not found.
       return false;
        
    }
    
    
    
}
