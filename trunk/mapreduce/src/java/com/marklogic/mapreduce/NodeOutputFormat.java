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
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * MarkLogicOutputFormat for Node.
 * 
 * <p>
 *  Use this class to store MapReduce results as XML nodes in a MarkLogic
 *  database. Use this class with output key-value pairs where the key is
 *  {@link NodePath} and the value is a {@link MarkLogicNode} to be 
 *  inserted into the database relative to the node path in the key.
 * </p>
 * <p>
 *  Where the node is inserted relative to the path is controlled by the
 *  {@link MarkLogicConstants#NODE_OPERATION_TYPE output.node.optype} configuration
 *  property. You must set this property when using {@link NodeOutputFormat}.
 * </p>
 * 
 * @see NodeOpType
 * @see MarkLogicConstants#NODE_OPERATION_TYPE
 * @author jchen
 */
public class NodeOutputFormat 
extends MarkLogicOutputFormat<NodePath, MarkLogicNode> {
    public static final Log LOG =
        LogFactory.getLog(NodeOutputFormat.class);
    
    @Override
    public RecordWriter<NodePath, MarkLogicNode> getRecordWriter(
            TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        LinkedMapWritable forestHostMap = 
            DefaultStringifier.load(conf, OUTPUT_FOREST_HOST, 
                    LinkedMapWritable.class);
        try {
            int taskId = context.getTaskAttemptID().getTaskID().getId();
            String host = InternalUtilities.getHost(taskId, forestHostMap);
            URI serverUri = InternalUtilities.getOutputServerUri(conf, host);
            return new NodeWriter(serverUri, conf);
        } catch (URISyntaxException e) {
            LOG.error(e);
            throw new IOException(e);
        }
    }

}
