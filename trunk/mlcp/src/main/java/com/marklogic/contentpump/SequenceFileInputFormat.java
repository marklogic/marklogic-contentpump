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
package com.marklogic.contentpump;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.marklogic.mapreduce.DocumentURI;

/**
 * InputFromat for SequenceFile.
 * @author ali
 *
 * @param <VALUE>
 */
public class SequenceFileInputFormat <VALUE> extends 
FileAndDirectoryInputFormat<DocumentURI, VALUE> {

    @Override
    public RecordReader<DocumentURI, VALUE> createRecordReader(
        InputSplit arg0, TaskAttemptContext arg1) throws IOException,
        InterruptedException {
        return new SequenceFileReader<VALUE>();
    }
    
}
