package com.datadio.storm.lib;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Signature {
	
	public static String getMD5(String doc) {
		String md5 = null;
		try {
			md5 = getMD5(doc.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return md5;
	}
	
	public static String getMD5(byte[] doc) {
		String md5 = null;
         
        try {
             
        	//Create MessageDigest object for MD5
        	MessageDigest digest = MessageDigest.getInstance("MD5");
        	
        	//Update input string in message digest
//            digest.update(str.getBytes("UTF-8"), 0, doc.length());
         
        	//Converts message digest value in base 16 (hex) 
//        	md5 = new BigInteger(1, digest.digest(doc)).toString(16);
        
        	// fix the 31 character problem
        	md5 = String.format("%032x", new BigInteger(1, digest.digest(doc)));
 
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        
        return md5;
	}
}
