/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * MarkLogicInputFormat for Node.
 * 
 * <p>
 *  Use this class when using XML nodes in a MarkLogic database as input
 *  to a MapReduce job. This format produces key-value pairs where the
 *  key is {@link NodePath} to the {@link MarkLogicNode} value.
 * </p>
 * 
 * @see com.marklogic.mapreduce.examples.LinkCountInDoc
 * @author jchen
 */
public class NodeInputFormat 
extends MarkLogicInputFormat<NodePath, MarkLogicNode> {
       
    @Override
    public RecordReader<NodePath, MarkLogicNode> createRecordReader(
            InputSplit arg0, TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new NodeReader(context.getConfiguration());
    }
}
