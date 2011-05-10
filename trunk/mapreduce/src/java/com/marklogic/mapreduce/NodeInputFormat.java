/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * MarkLogicInputFormat for Node.
 * 
 * @author jchen
 */
public class NodeInputFormat 
extends MarkLogicInputFormat<NodePath, MarkLogicNode> {
   	
	@Override
	public RecordReader<NodePath, MarkLogicNode> createRecordReader(
			InputSplit arg0, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new NodeReader(context.getConfiguration());
	}
}
