package com.datadio.storm.lib;

import java.util.List;
import java.util.Map;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;


import org.apache.commons.lang.StringUtils;

public class TweetScore {
	
	private final float EPS = 0.001f;
	
    public boolean isBetween(float x, float lower, float upper) {
      return lower <= x && x <= upper;
    }
    
    public boolean isEqual(float x, float y) {
        return Math.abs(x-y) < EPS;
      }

	public float get_author_score(Map<String, String> tweet) {
		//calculate author_score total 100
	    //based on: follow ratio = 60/100, avatar = 10/100, tweet/day = 30/100
	    float author_score = 0.0f;
	    int time_now = (int) (System.currentTimeMillis() / 1000L); // get unix timstamp;
        float followers;
        float following;
        float account_created = Float.parseFloat((String) tweet.get("account_created"));

	    float days_since_create_account = (time_now - account_created); //reg_date - current_date
	    days_since_create_account = days_since_create_account / (60.0f*60.0f*24.0f);
	    
//	    System.out.println(time_now);
//	    System.out.println("days" + days_since_create_account);

        try {
            followers = Float.parseFloat((String) tweet.get("followers"));
        } catch (Exception e) {
            followers = 0.0f;
        }

        try {
            following = Float.parseFloat((String) tweet.get("following"));
        } catch (Exception e) {
            following = 0.0f;
        }

	    if (following <= 10f) {
            // An active or “True” Twitter user has at least 10 
            // ref: http://mashable.com/2010/03/10/twitter-follow-stats/
            //     http://www.sysomos.com/insidetwitter/
            if ((followers / following) > 100f) {
                following = 50.0f; // some penalty
            } else {
                following = 30.0f;
            }
        }
	    
	    float follow_rato = followers / following;
	    
//	    System.out.println("followers" + followers);
//	    System.out.println("following" + following);
//	    System.out.println("follow_rato" + follow_rato);

	    // puts "follow_rato", follow_rato
	    float follow_rato_threshold = 200.0f; // decide when it will get full score from follow ratio
	    float follow_rato_factor = 50.0f / follow_rato_threshold;

	    if (follow_rato*follow_rato_factor < 50f) {
            author_score += follow_rato*follow_rato_factor;
        } else {
            author_score += 50.0f;
        }

	    if ("true".equals((String) tweet.get("default_profile_image")) ) {
            author_score -= 10.0f;
        } else {
            author_score += 10.0f;
        }

        if ("true".equals((String) tweet.get("default_profile")) ) {
            author_score -= 5.0f;
        } else {
            author_score += 5.0f;
        }

        float num_tweets = Float.parseFloat((String) tweet.get("tweets"));
	    float tweet_per_day = num_tweets / days_since_create_account;

        if (isBetween(tweet_per_day, 0, 4)) {
            author_score += 5;
        } else if (isBetween(tweet_per_day, 5, 9)) {
            author_score += 10;
        } else if (isBetween(tweet_per_day, 10, 20)) {
            author_score += 20;
        } else if (isBetween(tweet_per_day, 21, 50)) {
            author_score += 25;
        } else {
            author_score += 35;
        }

	    return author_score;
	}

    public float get_topic_score(List<String> users, List<String> competitors, Map<String, String> tweet) {
        //calculate topic_score total 100 ###
        //based on: times that keywords mentioned = 70/100, retweet times = 30/100
        float topic_score = 0.0f;
        int num_users = users.size();
        int num_competitors = competitors.size();

        String tweet_text = (String) tweet.get("tweet");
        tweet_text = tweet_text.toLowerCase();

        int user_mentioned = 0;
        int competitor_mentioned = 0;
        
        for(String user : users ){
//           String new_user = user.replaceAll("\\s", "( |-|)");
           //System.out.println("new_user" + new_user);
//           Pattern match_pattern = Pattern.compile("(?iu)(^|\\b|\\s)(" + new_user + ")(\\b|$)");
//           Matcher matcher = match_pattern.matcher(tweet_text);
//           if (matcher.find()) {
//                user_mentioned += 1;
//           }
           
           tweet_text = tweet_text.toLowerCase();
//           String new_keyword = user.replaceAll("\\s", "-");
//           String new_keyword2 = user.replaceAll("\\s", "");
           String new_keyword = StringUtils.replace(user, "\\s", "-");
           String new_keyword2 = StringUtils.replace(user, "\\s", "");
           if(tweet_text.contains(user.toLowerCase()) || 
					tweet_text.contains(new_keyword.toLowerCase()) || 
					tweet_text.contains(new_keyword2.toLowerCase())) {
				user_mentioned += 1;
           }
        }

        for(String competitor : competitors ){
//           String new_competitor = competitor.replaceAll("\\s", "( |-|)");
//           
//           //System.out.println("new_competitor" + new_competitor);
//           
//           Pattern match_pattern = Pattern.compile("(?iu)(^|\\b|\\s)(" + new_competitor + ")(\\b|$)");
//           
//           //System.out.println("match_pattern" + match_pattern);
//           
//           Matcher matcher = match_pattern.matcher(tweet_text);
//           if (matcher.find()) {
//                competitor_mentioned += 1;
//           }
           
           tweet_text = tweet_text.toLowerCase();
//           String new_keyword = competitor.replaceAll("\\s", "-");
//           String new_keyword2 = competitor.replaceAll("\\s", "");
           String new_keyword = StringUtils.replace(competitor, "\\s", "-");
           String new_keyword2 = StringUtils.replace(competitor, "\\s", "");
           if(tweet_text.contains(competitor.toLowerCase()) || 
					tweet_text.contains(new_keyword.toLowerCase()) || 
					tweet_text.contains(new_keyword2.toLowerCase())) {
        	   competitor_mentioned += 1;
           }
        }
        
	    //System.out.println("user_mentioned" + user_mentioned);
	    //System.out.println("competitor_mentioned" + competitor_mentioned);
	    
//	    user_mentioned = 1;
//	    competitor_mentioned = 2;
        
        float user_mention_rate;
        float competitor_mention_rate;
        
        if (num_users == 0) {
        	user_mention_rate = 0;
        } else {
        	user_mention_rate = (float) user_mentioned / num_users;
        }
        
        if (num_competitors == 0) {
        	competitor_mention_rate = 0;
        } else {
        	competitor_mention_rate = (float) competitor_mentioned / num_competitors;
        }
        
        // calculate topic score based on ratio of client mentions and competitor mentions
        if (isEqual(competitor_mention_rate, 0)) {
        	if( !isEqual(user_mention_rate, 0)) {
        		topic_score += user_mention_rate*70.0;
        	}
        } else {
        	topic_score += (user_mention_rate / competitor_mention_rate)*35.0;
        }
        
        float num_retweets = Float.parseFloat((String) tweet.get("retweets"));
        if (num_retweets < 30.0) {
            topic_score += (num_retweets+1)*1.0;
        } else {
            topic_score += 30;
        }
          
        return topic_score;
    }


    public float get_keywords_score(int kwd_num, int tagged_kwd_num) {
        // calculate the score for tweet by using the terms ( or keywords ) that provided by users.
        float total_score = 0.0f;

        if(kwd_num == 0) {
            return 0.0f;
        } // no terms are provided by users

        float ratio = (float) tagged_kwd_num*100.0f / (float) kwd_num*1.0f;

        if (isBetween(ratio, 0, 19)) {
            total_score = 10.0f;
        } else if (isBetween(ratio, 20, 39)) {
            total_score = 30.0f;
        } else if (isBetween(ratio, 40, 49)) {
            total_score = 60.0f;
        } else if (isBetween(ratio, 50, 69)) {
            total_score = 80.0f;
        } else if (isBetween(ratio, 70, 89)) {
            total_score = 90.0f;
        } else if (isBetween(ratio, 90, 100)) {
            total_score = 100.0f;
        }

        return total_score;

    }

    public float get_score(List<String> users, List<String> competitors, int kwd_num, int tagged_kwd_num, Map<String, String> tweet) {
        float total_score = 0.0f;
        float author_score = 0.0f;
        float topic_score = 0.0f;
        float keywords_score = 0.0f;
        boolean has_topic_socre = false;
        boolean has_kwd_socre = false;
        
        author_score = get_author_score(tweet);
        //System.out.println("author_score:" + author_score );
        
        if ( (!users.isEmpty()) || (!competitors.isEmpty())) {
        	topic_score = get_topic_score(users, competitors, tweet);
        	has_topic_socre = true;
        	//System.out.println("topic_score:" + topic_score );
        }

        if (kwd_num > 0) {
        	keywords_score = get_keywords_score(kwd_num, tagged_kwd_num);
        	has_kwd_socre = true;
        	//System.out.println("keywords_score:" + keywords_score );
        }
        
        if (has_topic_socre && !has_kwd_socre) {
            total_score = author_score*0.4f + topic_score*0.6f;
        } else if (!has_topic_socre && has_kwd_socre) {
            total_score = author_score*0.4f + keywords_score*0.6f;
        } else if (has_topic_socre && has_kwd_socre) {
            total_score = author_score*0.4f + topic_score*0.3f + keywords_score*0.3f;
        } else {
            total_score = author_score;
        }

        return total_score;
    }


//    public static void main(String[] args) throws Exception {
//        //test
//        List<String> users = new ArrayList<String>();
//        users.add("apple");
//        users.add("iphone 5");
//        users.add("ipad");
//
//        List<String> competitors = new ArrayList<String>();
//        competitors.add("google system");
//        competitors.add("android");
//
//        int kwd_num = 10;
//        int tagged_kwd_num = 3;
//
//        Map<String, String> tweet = new HashMap<String, String>();
//        tweet.put("account_created","1341860084");
//        tweet.put("followers","5691");
//        tweet.put("following","5389");
//        tweet.put("default_profile_image","false");
//        tweet.put("default_profile","false");
//        tweet.put("tweets","105590");
//        tweet.put("retweets","0");
//        tweet.put("tweet","iphone 5 is really good product :D. I'd prefer iphone over android, hate google system.");
//        
//        TweetScore tw = new TweetScore();
//        float total_score = tw.get_score(users, competitors, kwd_num, tagged_kwd_num, tweet);
//
//        System.out.println("Result is " + total_score );
//    }
}
