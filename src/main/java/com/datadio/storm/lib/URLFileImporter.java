package com.datadio.storm.lib;

import java.io.BufferedReader;
import java.io.FileReader;

import com.datadio.storm.storage.PageCassandra;
import com.datadio.storm.storage.PageRedis;

/**
 * Simple urls importer from txt file
 * @author stevenyue
 *
 */
public class URLFileImporter {
	public static void main(String[] args) throws Exception {
		
		int counter = 0;
		int urlKeyExpireSec;
		
		if(args.length < 4) {
			System.err.println("Invalid number of args");
			System.err.println("The following args are required:");
			System.err.println(" - arg1: A path of the file that contains urls.");
			System.err.println(" - arg2: Url of redis. E.g. \"localhost\" or \"10.0.0.10\" ");
			System.err.println(" - arg3: Url of cassandra. E.g. \"127.0.0.1:9160\" or \"10.0.0.152:9160, 10.0.0.154:9160, 10.0.0.150:9160\" ");
			System.err.println(" - arg4: Time in seconds that the url keys in redis would expire. E.g. 1200.");
			System.exit(1);
		}
		
		urlKeyExpireSec = Integer.parseInt(args[3]);
		
		PageCassandra cass = new PageCassandra(args[2]);
		PageRedis redis_conn = new PageRedis(args[1]);
		BufferedReader br = new BufferedReader(new FileReader(args[0]));
		
	    try {
	        String line = br.readLine();
	        
	        while (line != null) {	
	        	WebPage newPage = new WebPage(line);
				System.out.println("Adding: " + line + " : " + newPage.getUniqKey());
				cass.addNewPage(newPage);
				redis_conn.addToQueue(newPage, urlKeyExpireSec);
	            line = br.readLine();
	            counter++;
	        }
	    } finally {
	        br.close();
	    }
		
		System.out.println("Finished importing " + counter + " urls");
	}
}
