package com.marklogic.mapreduce;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.exceptions.XccConfigException;

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
    public static final Log LOG = LogFactory.getLog(ContentOutputFormat.class);
    
    // Prepend to a forest id to form a database name parsed by XDBC.
    // Also used here alone to indicate that non-fast-mode.
    static final String ID_PREFIX = "#";

    @Override
    public RecordWriter<DocumentURI, VALUEOUT> getRecordWriter(
            TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        LinkedMapWritable forestHostMap = 
            DefaultStringifier.load(conf, OUTPUT_FOREST_HOST, 
                    LinkedMapWritable.class);
        boolean fastLoad = conf.getBoolean(OUTPUT_FAST_LOAD, false) ||
                           (conf.get(OUTPUT_DIRECTORY) != null);
        if (fastLoad) {
            LOG.info("Running in fast load mode");
        }
        
        Map<String, ContentSource> sourceMap = 
            new LinkedHashMap<String, ContentSource>();
        if (fastLoad) {        
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
            for (Writable forestId : forestHostMap.keySet()) {
                String forest = ((Text)forestId).toString();
                Writable hostName = forestHostMap.get(forestId);
                ContentSource cs = hostSourceMap.get(hostName);
                sourceMap.put(ContentOutputFormat.ID_PREFIX + forest, cs);
            }           
        } else {
            // treating the non-fast-load case as a special case of the 
            // fast-load case with only one content source
            int taskId = context.getTaskAttemptID().getTaskID().getId();
            String host = InternalUtilities.getHost(taskId, forestHostMap);
            
            try {
                ContentSource cs = InternalUtilities.getOutputContentSource(
                    conf, host.toString());
                sourceMap.put(ContentOutputFormat.ID_PREFIX, cs);
            } catch (XccConfigException e) {
                throw new IOException(e);
            } catch (URISyntaxException e) {
                throw new IOException(e);
            }
        }
        
        // construct the ContentWriter
        return new ContentWriter<VALUEOUT>(conf, sourceMap, fastLoad);
    }
}
