/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.marklogic.xcc.ResultItem;

/**
 * MarkLogicRecordReader that fetches data from MarkLogic server and generates 
 * key value pairs, with the key being a system generated integer and value 
 * in user specified type.
 * 
 * @author jchen
 *
 * @param <KEYIN>
 * @param <VALUEIN>
 */
public class KeyValueReader<KEYIN, VALUEIN>
extends MarkLogicRecordReader<KEYIN, VALUEIN>
implements MarkLogicConstants {

	/**
	 * Current key.
	 */
	private KEYIN key;

	/**
	 * Current value.
	 */
	private VALUEIN value;
	
	public KeyValueReader(Configuration conf, String serverUri) {
		super(conf, serverUri);
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
	}

	@Override
	protected boolean nextResult(ResultItem result) {
		// TODO Auto-generated method stub
		return false;
	}
}
