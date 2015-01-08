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
package com.marklogic.contentpump;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.MarkLogicConstants;

/**
 * InputFormat for reading compressed documents from file systems. One zip entry
 * in a compressed file on the file system is one record (key value pair).
 * 
 * @author ali
 * 
 * @param <VALUE>
 */
public class CompressedDocumentInputFormat<VALUE> extends
FileAndDirectoryInputFormat<DocumentURI, VALUE> {
    public static final Log LOG = 
        LogFactory.getLog(CompressedDocumentReader.class);
    
	@SuppressWarnings("unchecked")
    @Override
	public RecordReader<DocumentURI, VALUE> createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
	    Configuration conf = arg1.getConfiguration();
	    boolean streaming = conf.getBoolean(
	            MarkLogicConstants.OUTPUT_STREAMING, false);
	    if (streaming) {
	        return (RecordReader<DocumentURI, VALUE>) 
	            new CompressedStreamingReader();
	    } else {
	        return new CompressedDocumentReader<VALUE>();
	    }
	}

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
 
}
