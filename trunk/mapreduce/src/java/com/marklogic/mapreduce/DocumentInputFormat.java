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
 * MarkLogicInputFormat for Document.
 * 
 * @author jchen
 */
public class DocumentInputFormat extends MarkLogicInputFormat<DocumentURI, MarkLogicNode> {

	static final float DOCUMENT_TO_FRAGMENT_RATIO = 1;
	
	@Override
	public RecordReader<DocumentURI, MarkLogicNode> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		String serverUri = getServerUriTemp(conf);
		return new DocumentReader(conf, serverUri);
	}
	
	@Override
	public float getDefaultRecordFragRatio() {
		return DOCUMENT_TO_FRAGMENT_RATIO;
	}

}
