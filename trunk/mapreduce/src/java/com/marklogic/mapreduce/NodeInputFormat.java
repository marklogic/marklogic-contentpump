/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * MarkLogicInputFormat for Node.
 * 
 * @author jchen
 */
public class NodeInputFormat extends MarkLogicInputFormat<NodePath, MarkLogicNode> {
    
	static final float NODE_TO_FRAGMENT_RATIO = 100;
	
	@Override
	public RecordReader<NodePath, MarkLogicNode> createRecordReader(
			InputSplit arg0, TaskAttemptContext context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		String serverUri = getServerUriTemp(conf);
		return new NodeReader(conf, serverUri);
	}

	@Override
	public float getDefaultRecordFragRatio() {
		return NODE_TO_FRAGMENT_RATIO;
	}

}
