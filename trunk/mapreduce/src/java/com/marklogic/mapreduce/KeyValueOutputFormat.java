/*
 * Copyright 2003-2015 MarkLogic Corporation
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.marklogic.mapreduce.utilities.InternalUtilities;
import com.marklogic.mapreduce.utilities.TextArrayWritable;
import com.marklogic.xcc.ContentSource;

/**
 * MarkLogicOutputFormat for user specified key and value types.
 * 
 * @author jchen
 *
 * @param <KEYOUT>
 * @param <VALUEOUT>
 */
public class KeyValueOutputFormat<KEYOUT, VALUEOUT> extends
        MarkLogicOutputFormat<KEYOUT, VALUEOUT> {

    @Override
    public RecordWriter<KEYOUT, VALUEOUT> getRecordWriter(
            TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        TextArrayWritable hosts = getHosts(conf);
        String host = InternalUtilities.getHost(hosts);
        return new KeyValueWriter<KEYOUT, VALUEOUT>(conf, host);
    }

    @Override
    public void checkOutputSpecs(Configuration conf, ContentSource cs) 
    throws IOException {
        // check for required configuration
        if (conf.get(OUTPUT_QUERY) == null) {
            throw new IllegalArgumentException(OUTPUT_QUERY + 
            " is not specified.");
        }
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
