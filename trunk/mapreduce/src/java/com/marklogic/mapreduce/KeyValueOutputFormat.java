/*
 * Copyright (c) 2003-2012 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

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
        LinkedMapWritable forestHostMap = getForestHostMap(conf);
        int taskId = context.getTaskAttemptID().getTaskID().getId();
        String host = InternalUtilities.getHost(taskId, forestHostMap);
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
    }
}
