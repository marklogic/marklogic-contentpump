package com.marklogic.mapreduce;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.exceptions.XccConfigException;

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
		MapWritable forestHostMap = 
			DefaultStringifier.load(conf, OUTPUT_FOREST_HOST, 
					MapWritable.class);
		
		// get host->contentSource mapping
        Map<Writable, ContentSource> hostSourceMap = 
        	new HashMap<Writable, ContentSource>();
        for (Writable hostName : forestHostMap.values()) {
        	if (hostSourceMap.get(hostName) == null) {
        		try {
        	        ContentSource cs = InternalUtilities.getOutputContentSource(
        	    		conf, hostName.toString());
        	        hostSourceMap.put(hostName, cs);
        		} catch (XccConfigException e) {
        			throw new IOException(e);
        		} catch (URISyntaxException e) {
        			throw new IOException(e);
        		}
        	}
        }
		
		// consolidate forest->host map and host-contentSource map to 
        // forest-contentSource map
        Map<String, ContentSource> forestSourceMap = 
        	new HashMap<String, ContentSource>();
		for (Writable forestId : forestHostMap.keySet()) {
			String forest = ((Text)forestId).toString();
			Writable hostName = forestHostMap.get(forestId);
			ContentSource cs = hostSourceMap.get(hostName);
			forestSourceMap.put("#" + forest, cs);
		}
		
		// construct the ContentWriter
		return new ContentWriter<VALUEOUT>(conf, forestSourceMap);
    } 
}
