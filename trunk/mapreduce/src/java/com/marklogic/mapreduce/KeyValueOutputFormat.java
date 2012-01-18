/*
 * Copyright (c) 2003-2012 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;

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
        LinkedMapWritable forestHostMap = 
            DefaultStringifier.load(conf, OUTPUT_FOREST_HOST, 
                    LinkedMapWritable.class);
        try {
            int taskId = context.getTaskAttemptID().getTaskID().getId();
            String host = InternalUtilities.getHost(taskId, forestHostMap);
            URI serverUri = InternalUtilities.getOutputServerUri(conf, host);
            return new KeyValueWriter<KEYOUT, VALUEOUT>(serverUri, conf);
        } catch (URISyntaxException e) {
            LOG.error(e);
            throw new IOException(e);
        }
    }

    @Override
    void checkOutputSpecs(Configuration conf, Session session,
            AdhocQuery query, ResultSequence result) throws RequestException {
        // check for required configuration
        if (conf.get(OUTPUT_QUERY) == null) {
            throw new IllegalArgumentException(OUTPUT_QUERY + 
            " is not specified.");
        }
        
    }

}
