/*
 * Copyright (c) 2003-2012 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.marklogic.xcc.ContentSource;

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
        LinkedMapWritable forestHostMap = getForestHostMap(conf);
        int taskId = context.getTaskAttemptID().getTaskID().getId();
        String host = InternalUtilities.getHost(taskId, forestHostMap);
        return new NodeWriter(conf, host);
    }

    @Override
    public void checkOutputSpecs(Configuration conf, ContentSource cs) 
    throws IOException {
        // warn against unsupported configuration
        if (conf.get(BATCH_SIZE) != null) {
            LOG.warn("Config entry for " +
                    "\"mapreduce.marklogic.output.batchsize\" is not " +
                    "supported for " + this.getClass().getName() + 
                    " and will be ignored.");
        }     
    }

}
