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
 * MarkLogicOutputFormat for Document Property.
 * 
 * @author jchen
 */
public class PropertyOutputFormat 
extends MarkLogicOutputFormat<DocumentURI, MarkLogicNode> {
	public static final Log LOG =
	    LogFactory.getLog(PropertyOutputFormat.class);
	
	@Override
	public RecordWriter<DocumentURI, MarkLogicNode> getRecordWriter(
			TaskAttemptContext context) throws IOException, InterruptedException {		
		Configuration conf = context.getConfiguration();
		try {
			String host = getHost(conf, context.getTaskAttemptID().getId());
			URI serverUri = getServerUri(conf, host);
			String propOpType = conf.get(PROPERTY_OPERATION_TYPE, 
					DEFAULT_PROPERTY_OPERATION_TYPE);
			return new PropertyWriter(serverUri, 
					PropertyOpType.valueOf(propOpType));
		} catch (URISyntaxException e) {
			LOG.error(e);
			throw new IOException(e);
		}
	}

}
