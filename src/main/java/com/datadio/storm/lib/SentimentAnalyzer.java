package com.datadio.storm.lib;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import weka.core.Attribute;
import weka.core.FastVector;

public class SentimentAnalyzer {
	
	private List<String> pos_sentences = null;
	private List<String> neg_sentences = null;
	
	
	public void load_data(String pos_data_path, String neg_data_path) {
		File pos_dir = new File(pos_data_path);
		File neg_dir = new File(neg_data_path);
		
		if(!pos_dir.isDirectory()) {
			throw new IllegalArgumentException(pos_dir.toString() +  "is not a valid directory");
		} else if (!neg_dir.isDirectory()) {
			throw new IllegalArgumentException(neg_dir.toString() +  "is not a valid directory");
		}
			
		InputStream fs = null;
		BufferedReader br = null;
		pos_sentences = new ArrayList<String>();
		neg_sentences = new ArrayList<String>();
		String tweet;
		
		for(File pos_file : pos_dir.listFiles()) {
			try {
				fs = new FileInputStream(pos_file);
				br = new BufferedReader(new InputStreamReader(fs, Charset.forName("UTF-8")));
				while ((tweet = br.readLine()) != null) {
//					System.out.println(tweet);
					pos_sentences.add(tweet.trim());
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		for(File neg_file : neg_dir.listFiles()) {
			try {
				fs = new FileInputStream(neg_file);
				br = new BufferedReader(new InputStreamReader(fs, Charset.forName("UTF-8")));
				while ((tweet = br.readLine()) != null) {
//					System.out.println(tweet);
					neg_sentences.add(tweet.trim());
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		if (fs != null) { 
			try { fs.close(); } catch(Throwable t) { /* ensure close happens */ } 
		}
		if (br != null) { 
			try { br.close(); } catch(Throwable t) { /* ensure close happens */ } 
		}
		
	}
	
	public void tokenize(String text) {
		text = StringUtils.replace(text, "@\\w+", "");
		text = StringUtils.replace(text, "@\\w+", "");
	}
	
	public void create_word_dict() {
		
	}
	
//	public void train() {
//		
//		if(pos_sentences == null || neg_sentences == null) {
//			throw new IllegalArgumentException("Load some data first!");
//		}
//		
//		// create class attribute
//		FastVector fvClassVal = new FastVector(2);
//		fvClassVal.addElement("Positive");
//		fvClassVal.addElement("Negative");
//		Attribute thisClassAttribute = new Attribute("ClassType", fvClassVal);
//		 
//		// create text attribute
//	    FastVector inputTextVector = null;  // null -> String type
//	    Attribute thisTextAttribute = new Attribute("text", inputTextVector);
//		
//		InputStream fs = null;
//		BufferedReader br = null;
//		String tweet;
//		
//		for(File pos_file : pos_dir.listFiles()) {
//			try {
//				fs = new FileInputStream(pos_file);
//				br = new BufferedReader(new InputStreamReader(fs, Charset.forName("UTF-8")));
//				while ((tweet = br.readLine()) != null) {
////					System.out.println(tweet);
//					thisTextAttribute.addStringValue(tweet);
//				}
//			} catch (Exception e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
//		
//		for(File neg_file : neg_dir.listFiles()) {
//			try {
//				fs = new FileInputStream(neg_file);
//				br = new BufferedReader(new InputStreamReader(fs, Charset.forName("UTF-8")));
//				while ((tweet = br.readLine()) != null) {
////					System.out.println(tweet);
//					thisTextAttribute.addStringValue(tweet);
//				}
//			} catch (Exception e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
//		
//		if (fs != null) { 
//			try { fs.close(); } catch(Throwable t) { /* ensure close happens */ } 
//		}
//		if (br != null) { 
//			try { br.close(); } catch(Throwable t) { /* ensure close happens */ } 
//		}
//	    
//		
//		// create the attribute information
//		FastVector thisAttributeInfo = new FastVector(2);
//		thisAttributeInfo.addElement(thisTextAttribute);
//		thisAttributeInfo.addElement(thisClassAttribute);
//	}
	
//	public static void main(String[] args) throws Exception {
//		SentimentAnalyzer senti = new SentimentAnalyzer();
//		String pos_path = System.getProperty("user.dir") + "/" + "data/positive";
//		String neg_path = System.getProperty("user.dir") + "/" + "data/negative";
//		senti.train_data(pos_path, neg_path);
//	}
}
