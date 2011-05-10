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
			String host = getHost(conf, context.getTaskAttemptID().getId());
			URI serverUri = InternalUtilities.getOutputServerUri(conf, host);
			/* TODO: get host->forest mapping when 13333 is done.
			List<Long> forestIds = null;
			Collection<String> hostForests = conf.getStringCollection(OUTPUT_HOST_FORESTS); 			
			for (String entry : hostForests) {
				if (forestIds == null) {
					if (entry.equals(host)) {
						forestIds = new ArrayList<Long>();
					}
				} else {
					try {
					    long forestId = Long.parseLong(entry);
					    forestIds.add(forestId);
					} catch (NumberFormatException ex) {
						break; // stop looking
					}
				} 
			} */
			return new ContentWriter<VALUEOUT>(serverUri, conf);
		} catch (URISyntaxException e) {
			LOG.error(e);
			throw new IOException(e);
		}
    }

}
