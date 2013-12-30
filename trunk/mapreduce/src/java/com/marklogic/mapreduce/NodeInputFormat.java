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
