/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.types.ValueType;
import com.marklogic.xcc.types.XSInteger;

/**
 * <p>MarkLogicRecordReader that fetches data from MarkLogic server and generates 
 * key value pairs in user specified types.</p>
 * 
 * <p>Currently only support Text as KEYIN, and only support Text and 
 * IntWritable as VALUEIN class.</p>
 * 
 * @author jchen
 *
 * @param <KEYIN>
 * @param <VALUEIN>
 */
public class KeyValueReader<KEYIN, VALUEIN>
extends MarkLogicRecordReader<KEYIN, VALUEIN>
implements MarkLogicConstants {
	
	public static final Log LOG =
	    LogFactory.getLog(KeyValueReader.class);
	/**
	 * Current key.
	 */
	private KEYIN key;

	/**
	 * Current value.
	 */
	private VALUEIN value;
	private Class<?> keyClass;
	private Class<?> valueClass;
	
	/**
	 * Indicate whether a key has been fetched, and is waiting to fetch value
	 * from the next result.
	 */
	private boolean keyFetched;
	
	public KeyValueReader(Configuration conf, String serverUri) {
		super(conf, serverUri);
		keyClass = conf.getClass(INPUT_KEY_CLASS, Text.class);
		LOG.info("Key class " + keyClass);
		valueClass = conf.getClass(INPUT_VALUE_CLASS, Text.class);
		LOG.info("Value class " + valueClass);
	}

	@Override
	public VALUEIN getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public KEYIN getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	protected void endOfResult() {
		key = null;
		value = null;
		keyFetched = false;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected boolean nextResult(ResultItem result) {
		if (!keyFetched) {
			if (key == null) {
				key = (KEYIN)ReflectionUtils.newInstance(keyClass, 
						getConf());
			}
			if (keyClass.equals(Text.class)) {
				((Text)key).set(result.asString());
			} else {
				throw new UnsupportedOperationException("Key class " +  
						keyClass + " is unsupported for result type: " + 
						result.getValueType());
			}
		} else {
			if (value == null) {
				value = (VALUEIN)ReflectionUtils.newInstance(valueClass, 
						getConf());
			}
			if (valueClass.equals(Text.class)) {
				((Text)value).set(result.asString());
			} else if (valueClass.equals(IntWritable.class) &&
					result.getValueType() == ValueType.XS_INTEGER) {
				XSInteger intItem = (XSInteger)result.getItem();
				((IntWritable)value).set(intItem.asInteger());
			} else {
				throw new UnsupportedOperationException("Value class " +  
						valueClass + " is unsupported for result type: " + 
						result.getValueType());
			}
		}
		keyFetched = !keyFetched;
		return true;
	}
}
