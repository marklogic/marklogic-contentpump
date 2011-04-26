package com.marklogic.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
			URI serverUri = getServerUri(conf, host);
			List<Long> forestIds = null;
			/* TODO: get host->forest mapping when 13333 is done.
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
			String contentType = conf.get(CONTENT_TYPE, DEFAULT_CONTENT_TYPE);
			return new ContentWriter<VALUEOUT>(serverUri, 
					conf.get(OUTPUT_DIRECTORY),
					conf.getStrings(OUTPUT_COLLECTION), 
					conf.getStrings(OUTPUT_PERMISSION),
					conf.get(OUTPUT_QUALITY),
					ContentType.valueOf(contentType),
					forestIds);
		} catch (URISyntaxException e) {
			LOG.error(e);
			throw new IOException(e);
		}
    }

}
