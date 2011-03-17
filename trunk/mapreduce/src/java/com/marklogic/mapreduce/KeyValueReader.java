package com.marklogic.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.marklogic.xcc.ResultItem;

public class KeyValueReader<KEYIN, VALUEIN>
extends MarkLogicRecordReader<KEYIN, VALUEIN>
implements MarkLogicConstants {

	/**
	 * Current key.
	 */
	private KEYIN currentKey;

	/**
	 * Current value.
	 */
	private VALUEIN currentValue;
	
	public KeyValueReader(Configuration conf, String serverUri) {
		super(conf, serverUri);
	}

	@Override
	public VALUEIN getCurrentValue() throws IOException, InterruptedException {
		return currentValue;
	}

	@Override
	public KEYIN getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected void endOfResult() {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected boolean nextResult(ResultItem result) {
		// TODO Auto-generated method stub
		return false;
	}
}
