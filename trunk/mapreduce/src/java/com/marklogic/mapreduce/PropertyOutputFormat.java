/*
 * Copyright 2003-2013 MarkLogic Corporation
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
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.marklogic.mapreduce.utilities.InternalUtilities;
import com.marklogic.xcc.ContentSource;

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
 *  {@link MarkLogicConstants#DEFAULT_PROPERTY_OPERATION_TYPE output.property.optype}.
 *  By default, any existing properties are replaced with the new one.
 * </p>
 * <p>
 *  By default, properties are only created by documents that exist in the
 *  database. Set the configuration property
 *  {@link MarkLogicConstants#OUTPUT_PROPERTY_ALWAYS_CREATE output.property.alwayscreate}
 *  to true to create properties even if the target document does not exist.
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
        ArrayWritable hosts = getHosts(conf);
        
        int taskId = context.getTaskAttemptID().getTaskID().getId();
        String host = InternalUtilities.getHost(taskId, hosts);
        return new PropertyWriter(conf, host);
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
