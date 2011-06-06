/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * <p>MarkLogicInputFormat for a value other than document or node with
 * user specified key and value type. </p>
 * 
 * <p>Currently only support Text as KEYIN and VALUEIN class.</p>
 * 
 * @author jchen
 *
 * @param <KEYIN>
 * @param <VALUEIN>
 */

public class KeyValueInputFormat<KEYIN, VALUEIN> 
extends MarkLogicInputFormat<KEYIN, VALUEIN>
implements MarkLogicConstants {

    @Override
    public RecordReader<KEYIN, VALUEIN> createRecordReader(InputSplit split,
            TaskAttemptContext context) 
    throws IOException, InterruptedException {
        return new KeyValueReader<KEYIN, VALUEIN>(context.getConfiguration());
    }

}
