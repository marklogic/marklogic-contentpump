/*
 * Copyright (c) 2003-2012 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * MarkLogicInputFormat for Document.
 * 
 * <p>
 *  Use this class when using documents in a MarkLogic Database as input
 *  in a MapReduce job. This format produces key-value pairs where the
 *  key is the {@link DocumentURI} and the value is a document in VALUEIN 
 *  at the given URI. 
 * </p>
 * 
 * @see com.marklogic.mapreduce.examples.ContentReader
 * @author jchen
 */
public class DocumentInputFormat<VALUEIN>
extends MarkLogicInputFormat<DocumentURI, VALUEIN> {
    
    @Override
    public RecordReader<DocumentURI, VALUEIN> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new DocumentReader<VALUEIN>(context.getConfiguration());
    }
}
