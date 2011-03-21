/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * MarkLogicOutputFormat for Node.
 * 
 * @author jchen
 */
public class NodeOutputFormat extends MarkLogicOutputFormat<NodePath> {
	public static final Log LOG =
	    LogFactory.getLog(NodeOutputFormat.class);
	
	@Override
	public RecordWriter<NodePath, MarkLogicNode> getRecordWriter(
			TaskAttemptContext context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		try {
			URI serverUri = getServerUri(conf);
			return new NodeWriter(serverUri, 
					conf.getStringCollection(OUTPUT_NAMESPACE),
					conf.get(NODE_OPERATION_TYPE));
		} catch (URISyntaxException e) {
			LOG.error(e);
			throw new IOException(e);
		}
	}

}
