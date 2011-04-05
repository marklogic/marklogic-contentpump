package com.marklogic.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * MarkLogicOutputFormat for Content.
 * 
 * @author jchen
 *
 * @param <VALUEOUT>
 */
public class ContentOutputFormat<VALUEOUT> extends
        MarkLogicOutputFormat<DocumentURI, VALUEOUT> {

	@Override
    public RecordWriter<DocumentURI, VALUEOUT> getRecordWriter(
            TaskAttemptContext context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		try {
			URI serverUri = getServerUri(conf);
			return new ContentWriter<VALUEOUT>(serverUri, 
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
