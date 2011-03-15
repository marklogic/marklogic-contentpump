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
 * MarkLogicOutputFormat for Document.
 *
 * @author jchen
 */
public class DocumentOutputFormat extends MarkLogicOutputFormat<DocumentURI> {
	public static final Log LOG =
	    LogFactory.getLog(DocumentOutputFormat.class);
	
	@Override
	public RecordWriter<DocumentURI, MarkLogicRecord> getRecordWriter(
			TaskAttemptContext context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		try {
			URI serverUri = getServerUri(conf);
			return new DocumentWriter(serverUri, 
					conf.get(OUTPUT_DIRECTORY),
					conf.getStrings(OUTPUT_COLLECTION), 
					conf.getStrings(OUTPUT_PERMISSION),
					conf.get(OUTPUT_QUALITY));
		} catch (URISyntaxException e) {
			LOG.error(e);
			throw new IOException(e);
		}
	}
}
