package com.marklogic.mapreduce;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.exceptions.XccConfigException;
import com.marklogic.xcc.types.XSBoolean;

/**
 * MarkLogicOutputFormat for Content.
 * 
 * <p>
 *  Use this class to store results as content in a MarkLogic Server database.
 *  The text, XML, or binary content is inserted into the database at the
 *  given {@link DocumentURI}.
 * </p>
 * <p>
 *  When using this {@link MarkLogicOutputFormat}, your key should be the URI of
 *  the document to insert into the database. The value should be the content to
 *  insert, in the form of {@link org.apache.hadoop.io.Text} or 
 *  {@link MarkLogicNode}.
 * </p>
 * <p>
 *  Several configuration properties exist for controlling the content insertion,
 *  including permissions, collections, quality, directory, and content type.
 * </p>
 * 
 * @see MarkLogicConstants
 * @see com.marklogic.mapreduce.examples.ContentLoader
 * @see com.marklogic.mapreduce.examples.ZipContentLoader
 * @author jchen
 *
 * @param <VALUEOUT>
 */
public class ContentOutputFormat<VALUEOUT> extends
        MarkLogicOutputFormat<DocumentURI, VALUEOUT> {
    
    @Override
    public RecordWriter<DocumentURI, VALUEOUT> getRecordWriter(
            TaskAttemptContext context) throws IOException, InterruptedException {
        LinkedMapWritable forestHostMap = 
            DefaultStringifier.load(conf, OUTPUT_FOREST_HOST, 
                    LinkedMapWritable.class);
        
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
            new LinkedHashMap<String, ContentSource>();
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
