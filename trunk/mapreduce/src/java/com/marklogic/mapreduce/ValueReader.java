/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

import com.marklogic.xcc.ResultItem;

/**
 * <p>A RecordReader that fetches data from MarkLogic server and generates 
 * an integer key for each value fetched.</p>
 * 
 * <p>Currently only support Text as ValueClass.</p>
 * 
 * @author jchen
 * 
 * @param <VALUEIN>
 */
@SuppressWarnings("deprecation")
public class ValueReader<VALUEIN> 
extends MarkLogicRecordReader<LongWritable, VALUEIN>
implements MarkLogicConstants {

	public static final Log LOG =
	    LogFactory.getLog(ValueReader.class);
	private LongWritable key;
	private VALUEIN value;
	private Class<?> valueClass;

	public ValueReader(Configuration conf, String serverUriTemp) {
		super(conf, serverUriTemp);
		valueClass = conf.getClass(INPUT_KEY_CLASS, Text.class);
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public VALUEIN getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	protected void endOfResult() {
		key = null;
		value = null;
	}

    @SuppressWarnings("unchecked")
    @Override
	protected boolean nextResult(ResultItem result) {
		if (key == null) {
			key = new LongWritable(getCount());
		} else {
			key.set(getCount());
		}
		if (value == null) {
			value = (VALUEIN)ReflectionUtils.newInstance(valueClass, 
					getConf());
		}
		if (valueClass.equals(Text.class)) {
			((Text)value).set(result.asString());
		} else {
			throw new UnsupportedOperationException("Value class " +  
					valueClass + " is unsupported for result type: " + 
					result.getValueType());
		}
		return true;
	}

}
