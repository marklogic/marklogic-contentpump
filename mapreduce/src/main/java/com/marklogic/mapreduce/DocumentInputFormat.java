/*
 * Copyright 2003-2016 MarkLogic Corporation

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
        return new DocumentReader<>(context.getConfiguration());
    }
}
