/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * MarkLogicInputFormat for Document.
 * 
 * @author jchen
 */
public class DocumentInputFormat 
extends MarkLogicInputFormat<DocumentURI, MarkLogicNode> {
    
    @Override
    public RecordReader<DocumentURI, MarkLogicNode> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new DocumentReader(context.getConfiguration());
    }
}
