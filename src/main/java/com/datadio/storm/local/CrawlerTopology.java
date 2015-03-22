package com.datadio.storm.local;

import com.datadio.storm.fetcher.RSSFetchBolt;
import com.datadio.storm.fetcher.URLAsyncFetchBolt;
import com.datadio.storm.lib.DioConfig;
import com.datadio.storm.parser.ContentKeywordsBolt;
import com.datadio.storm.parser.ContentParseBolt;
import com.datadio.storm.parser.ContentScoreBolt;
import com.datadio.storm.parser.RSSParseBolt;
import com.datadio.storm.parser.URLParseBolt;
import com.datadio.storm.scheduler.RSSGenerateBolt;
import com.datadio.storm.scheduler.URLGenerateBolt;
import com.datadio.storm.scheduler.URLPartitionBolt;
import com.datadio.storm.spout.RSSSpout;
import com.datadio.storm.spout.RequeueSpout;
import com.datadio.storm.spout.URLSpout;
import com.datadio.storm.storage.ContentFinalizeBolt;
import com.datadio.storm.storage.URLFinalizeBolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class CrawlerTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        DioConfig dioConfig = new DioConfig();
        Config conf = new Config();
//        String topLevel = "development";
        String topLevel = "production";
        
        if("development".equals(topLevel)) {
        	
            // for local
        	conf.setDebug(false);
        	conf.put(Config.TOPOLOGY_WORKER_SHARED_THREAD_POOL_SIZE, 100);
        	
        	dioConfig.setCassandraHost("127.0.0.1:9160");
        	dioConfig.setRedisHost("localhost");
        	dioConfig.setSqlHost("jdbc:mysql://localhost:3306", "socialmon_develop_test", "root", "abc123");
            dioConfig.setUrlKeyExpireSec(60*30);
            dioConfig.put("defaultDomainInterval", 5);
            dioConfig.put("initDomainInterval", 2);
            dioConfig.put("defaultPageInterval", 2);
            dioConfig.put("domainSpoutNum", 5); // number of domains the spout will grab from redis at each time.
            dioConfig.put("urlSpoutNum", 10); // number of url from each domain to grab
            
            builder.setSpout("0-URLSpout", new URLSpout(dioConfig), 1);
            
            builder.setSpout("00-RSSSpout", new RSSSpout(dioConfig), 1);
            
//            builder.setSpout("000-ReQueueSpout", new RequeueSpout(dioConfig), 1);
            
            builder.setBolt("01-RSSGenerator", new RSSGenerateBolt(dioConfig), 2)
    				.shuffleGrouping("00-RSSSpout");
            
            builder.setBolt("02-RSSFetcher", new RSSFetchBolt(), 2)
            		.shuffleGrouping("01-RSSGenerator");
            
            builder.setBolt("03-RSSParser", new RSSParseBolt(dioConfig), 2)
    				.shuffleGrouping("02-RSSFetcher");
            
            
            builder.setBolt("1-Generator", new URLGenerateBolt(dioConfig), 2)
                    .shuffleGrouping("0-URLSpout");
            
            builder.setBolt("2-Partitioner", new URLPartitionBolt(dioConfig), 2)
            		.fieldsGrouping("1-Generator", new Fields("domain"));
            
//            builder.setBolt("3-Fetcher", new URLFetchBolt(), 120)
            builder.setBolt("3-Fetcher", new URLAsyncFetchBolt(), 2)
            		.shuffleGrouping("2-Partitioner")
            		.shuffleGrouping("03-RSSParser");
            
            builder.setBolt("4-Parser", new URLParseBolt(dioConfig), 2)
    				.shuffleGrouping("3-Fetcher", "LinkDiscover");
            
            builder.setBolt("5-ContentParser", new ContentParseBolt(), 2)
					.shuffleGrouping("3-Fetcher", "ContentParser");
            
            builder.setBolt("5.1-ContentKeywords", new ContentKeywordsBolt(dioConfig), 2)
					.shuffleGrouping("5-ContentParser");
            
            builder.setBolt("5.2-ContentScore", new ContentScoreBolt(dioConfig), 2)
					.shuffleGrouping("5.1-ContentKeywords");
            
            builder.setBolt("5.3-ContentFinalize", new ContentFinalizeBolt(dioConfig), 2)
					.shuffleGrouping("5.2-ContentScore");
            
            builder.setBolt("6-Final", new URLFinalizeBolt(dioConfig), 2)
    				.shuffleGrouping("4-Parser");
        	
        } else {
            // for production
        	conf.setDebug(false);
            dioConfig.setCassandraHost("10.0.0.152:9160, 10.0.0.154:9160, 10.0.0.150:9160");
            dioConfig.setSqlHost("jdbc:mysql://10.0.0.10:3306", "datadio", "datadio", "j38h78237fg32");
            dioConfig.setRedisHost("10.0.0.10");
            dioConfig.setUrlKeyExpireSec(60*20);
            dioConfig.put("defaultDomainInterval", 5);
            dioConfig.put("initDomainInterval", 2);
            dioConfig.put("defaultPageInterval", 2);
            dioConfig.put("domainSpoutNum", 500);
            dioConfig.put("urlSpoutNum", 1000);
            
            builder.setSpout("0-URLSpout", new URLSpout(dioConfig), 1);
            
            builder.setSpout("00-RSSSpout", new RSSSpout(dioConfig), 1);
            
            builder.setSpout("000-ReQueueSpout", new RequeueSpout(dioConfig), 1);
            
            builder.setBolt("01-RSSGenerator", new RSSGenerateBolt(dioConfig), 12)
    				.shuffleGrouping("00-RSSSpout");
            
            builder.setBolt("02-RSSFetcher", new RSSFetchBolt(), 30)
            		.shuffleGrouping("01-RSSGenerator");
            
            builder.setBolt("03-RSSParser", new RSSParseBolt(dioConfig), 12)
    				.shuffleGrouping("02-RSSFetcher");
            
            builder.setBolt("1-Generator", new URLGenerateBolt(dioConfig), 12)
                    .shuffleGrouping("0-URLSpout");
            
            builder.setBolt("2-Partitioner", new URLPartitionBolt(dioConfig), 12)
            		.fieldsGrouping("1-Generator", new Fields("domain"));
            
//            builder.setBolt("3-Fetcher", new URLFetchBolt(), 120)
            builder.setBolt("3-Fetcher", new URLAsyncFetchBolt(), 30)
            		.shuffleGrouping("2-Partitioner")
            		.shuffleGrouping("03-RSSParser");
            
            builder.setBolt("4-Parser", new URLParseBolt(dioConfig), 12)
    				.shuffleGrouping("3-Fetcher", "LinkDiscover");
            
            builder.setBolt("5-ContentParser", new ContentParseBolt(), 12)
					.shuffleGrouping("3-Fetcher", "ContentParser");
            
            builder.setBolt("5.1-ContentKeywords", new ContentKeywordsBolt(dioConfig), 12)
					.shuffleGrouping("5-ContentParser");
            
            builder.setBolt("5.2-ContentScore", new ContentScoreBolt(dioConfig), 12)
					.shuffleGrouping("5.1-ContentKeywords");
            
            builder.setBolt("5.3-ContentFinalize", new ContentFinalizeBolt(dioConfig), 12)
					.shuffleGrouping("5.2-ContentScore");
            
            builder.setBolt("6-Final", new URLFinalizeBolt(dioConfig), 30)
    				.shuffleGrouping("4-Parser");
        }
    
        // in production
        // run it like:
        // storm jar path/to/allmycode.jar com.datadio.storm.local.TweetItemNewTopology name_of_topology
        if(args!=null && args.length > 0) {
        
            conf.setDebug(false);
        	
            conf.setNumWorkers(6);
            conf.setNumAckers(12);
            conf.setMaxSpoutPending(10000);
            
//            conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE,16384);
//        	conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE,16384);
        	conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE,2048);
         	conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE,2048);
        	conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
        	conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
        	conf.put(Config.WORKER_CHILDOPTS, "-Xmx8192m");
        	conf.put(Config.TOPOLOGY_WORKER_SHARED_THREAD_POOL_SIZE, 750);
        	
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
//            Utils.sleep(60000);
//            cluster.killTopology("test");
//            cluster.shutdown();
            
        }
    }
}
