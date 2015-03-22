package com.datadio.storm.lib;

import java.io.UnsupportedEncodingException;

import java.util.Collections;
import java.util.List;

import org.json.simple.JSONValue;

import backtype.storm.spout.Scheme;

import backtype.storm.tuple.Fields;

/**
 * Deserialization scheme for JSON values using the json-simple library. Emits
 * one-element tuples with the field name <tt>object</tt>, containing the parsed
 * JSON value.
 * 
 * @author Sam Stokes (sam@rapportive.com)
 * @see <a href="http://code.google.com/p/json-simple/">json-simple</a>
 */
public class SimpleJSONScheme implements Scheme {
	private static final long serialVersionUID = -7734176307841199017L;

	private final String encoding;

	/**
	 * Create a new JSON deserialization scheme using the given encoding.
	 * 
	 * @param encoding
	 *            character encoding used to deserialize JSON from raw bytes
	 */
	public SimpleJSONScheme(String encoding) {
		this.encoding = encoding;
	}

	/**
	 * Create a new JSON deserialization scheme using UTF-8 as encoding.
	 */
	public SimpleJSONScheme() {
		this("UTF-8");
	}

	/**
	 * Deserialize a JSON value from <tt>bytes</tt> using the previously
	 * specified encoding.
	 * 
	 * @return a one-element tuple containing the parsed JSON value.
	 * 
	 * @throws IllegalStateException
	 *             if the requested character encoding is not supported.
	 */
	public List<Object> deserialize(byte[] bytes) throws IllegalStateException {
		final String chars;
		try {
			chars = new String(bytes, encoding);
		} catch (UnsupportedEncodingException e) {
			throw new IllegalStateException(e);
		}
		final Object object = JSONValue.parse(chars);
		if (object != null) {
			return Collections.singletonList(object);
		} else {
			return Collections.emptyList();
		}
	}

	/**
	 * Emits tuples containing only one field, named "object".
	 */
	public Fields getOutputFields() {
		return new Fields("object");
	}
}