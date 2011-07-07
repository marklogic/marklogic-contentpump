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
 * <p>
 *  Use this class to store MapReduce results as properties on documents
 *  in a MarkLogic database. This class expects output key-value pairs
 *  where the key is a {@link DocumentURI} and the value is a {@link MarkLogicNode}
 *  describing the property to be added to the document at the key URI.
 * </p>
 * <p>
 *  Control whether the inserted property replaces or adds to existing
 *  document properties by setting the configuration property
 *  <code>mapreduce.marklogic.output.propertyoptype</code>.
 *  By default, any existing properties are replaced with the new one.
 * </p>
 * 
 * @see PropertyOpType
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
            int taskId = context.getTaskAttemptID().getTaskID().getId();
            String host = getHost(conf, taskId);
            URI serverUri = InternalUtilities.getOutputServerUri(conf, host);
            return new PropertyWriter(serverUri, conf);
        } catch (URISyntaxException e) {
            LOG.error(e);
            throw new IOException(e);
        }
    }

}
