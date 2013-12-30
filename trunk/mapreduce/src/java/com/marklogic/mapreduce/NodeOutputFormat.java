/*
 * Copyright 2003-2014 MarkLogic Corporation

 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.marklogic.mapreduce.utilities.InternalUtilities;
import com.marklogic.mapreduce.utilities.TextArrayWritable;
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
        TextArrayWritable hosts = getHosts(conf);
        int taskId = context.getTaskAttemptID().getTaskID().getId();
        String host = InternalUtilities.getHost(taskId, hosts);
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
        // store hosts into config system
        DefaultStringifier.store(conf, queryHosts(cs), OUTPUT_FOREST_HOST);
    }

}
