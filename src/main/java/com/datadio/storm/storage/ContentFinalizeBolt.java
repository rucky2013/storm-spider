package com.datadio.storm.storage;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.datadio.storm.lib.DioCassandra;
import com.datadio.storm.lib.DioConfig;
import com.datadio.storm.lib.DioRedis;
import com.datadio.storm.lib.MD5Signature;
import com.datadio.storm.lib.WebPage;
import com.datadio.storm.scheduler.FetchScheduler;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

enum ErrorCode {
    NoTextError, FormatError, FileLoadError, DuplicateLangError, NeedLoadProfileError, CantDetectError, CantOpenTrainData, TrainDataFormatError, InitParamError
}

public class ContentFinalizeBolt extends BaseRichBolt{
	private static final long serialVersionUID = 1L;
	
	OutputCollector _collector;
	private final String cassHost;
	private final String redisHost;
	private final int urlKeyExpireSec;
	private final int defaultPageInterval;
	DioCassandra cass_conn = null;
	DioRedis redis_conn = null;
	FetchScheduler scheduler;
	private boolean profileLoaded = false;
	private static final Logger LOG = LoggerFactory.getLogger(ContentFinalizeBolt.class);
	
    public ContentFinalizeBolt(DioConfig config) {
    	redisHost = config.getRedisHost();
    	cassHost = config.getCassandraHost();
    	urlKeyExpireSec = config.getUrlKeyExpireSec();
    	defaultPageInterval = (Integer) config.get("defaultPageInterval");
    }
	
    @SuppressWarnings("rawtypes")
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    	_collector = collector;
        cass_conn = new DioCassandra(cassHost);
        redis_conn = new DioRedis(redisHost);
        scheduler = new FetchScheduler();
        
    	String profileDirectory = System.getProperty("user.dir") + "/" + "data/profiles";
        try {
 			DetectorFactory.loadProfile(profileDirectory);
 			profileLoaded = true;
 		} catch (LangDetectException e) {
 			// stupid design can not get e.getCode() even though it's public!
 			// have to check string.
 			if(e.getMessage().contains("duplicate")) {
 				profileLoaded = true;
 			} else {
 				LOG.error("Failed to load language detection profile", e);
 				LOG.error("*** The language profile path should be: " + profileDirectory);
 			}
 		}
    }

    @SuppressWarnings("unchecked")
	public void execute(Tuple tuple) {
//    	WebPage originPage = (WebPage)tuple.getValue(0);
    	WebPage originPage = new WebPage((Map<String, Object>)tuple.getValue(0));
    	Integer pid = (Integer)tuple.getValue(1);
    	scheduler.updateFetchScheduler(originPage);
    	
    	Detector detector;
    	String lang = "";
    	
    	if(profileLoaded) {
			try {
				detector = DetectorFactory.create();
				detector.append(originPage.getMainContent());
			    lang = detector.detect();
			} catch (LangDetectException e1) {
				LOG.error("Failed to detect language", e1);
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
			}
    	} else {
    		LOG.error("Language detction profile is not loaded correctly");
    	}
    	
    	LOG.info("**** Detected language:" + lang);
    	
    	cass_conn.insertBlogItem(originPage, lang);
    	String iid = MD5Signature.getMD5(originPage.getCleanedUrl());
    	try {
			cass_conn.insertBlogEntry(pid, iid, originPage.getScore(), originPage.getPublishedDate(), originPage.getTaggedKeywords());
			redis_conn.aggregate_blog_record(originPage, pid, originPage.getTaggedKeywords());
		} catch (UnsupportedEncodingException e) {
			LOG.error(e.getMessage(), e);
		}
    	
        _collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("webpage"));
    }
}
