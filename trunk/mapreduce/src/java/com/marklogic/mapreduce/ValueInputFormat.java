/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * <p>MarkLogicInputFormat for a value other than document or node with 
 * system-generated key.</p>
 * 
 * <p>Currently only support Text as ValueClass.</p>
 * 
 * @author jchen
 *
 * @param <VALUEIN>
 */
public class ValueInputFormat<VALUEIN> 
extends MarkLogicInputFormat<LongWritable, VALUEIN> 
implements MarkLogicConstants {
	static final float VALUE_TO_FRAGMENT_RATIO = 100;
	
	@Override
	public float getDefaultRecordFragRatio() {
		return VALUE_TO_FRAGMENT_RATIO;
	}

	@Override
	public RecordReader<LongWritable, VALUEIN> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		return new ValueReader<VALUEIN>(conf, getServerUriTemp(conf));
	}

}
