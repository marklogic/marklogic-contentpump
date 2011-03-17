package com.marklogic.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

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
