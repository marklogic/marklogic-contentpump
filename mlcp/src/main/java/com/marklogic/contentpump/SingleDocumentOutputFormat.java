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
package com.marklogic.contentpump;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.MarkLogicDocument;

/**
 * OutputFormat for DocumentURI and MarkLogicDocument creating a single file.
 * 
 * @author ali
 */
public class SingleDocumentOutputFormat extends 
FileOutputFormat<DocumentURI, MarkLogicDocument> {
   
    @Override
    public RecordWriter<DocumentURI, MarkLogicDocument> getRecordWriter(
        TaskAttemptContext contex) throws IOException, InterruptedException {
        Configuration conf = contex.getConfiguration();
        String p = conf.get(ConfigConstants.CONF_OUTPUT_FILEPATH);
        Path path = new Path(p);
        return new SingleDocumentWriter(path, conf);
    }
    
    @Override
    public synchronized 
    OutputCommitter getOutputCommitter(TaskAttemptContext context
                                       ) throws IOException {
        return new OutputCommitter() {
            @Override
            public void abortTask(TaskAttemptContext taskContext) { }
            @Override
            public void commitTask(TaskAttemptContext taskContext) { }
            @Override
            public boolean needsTaskCommit(TaskAttemptContext taskContext) {
              return false;
            }
            @Override
            public void setupJob(JobContext jobContext) { }
            @Override
            public void setupTask(TaskAttemptContext taskContext) { }
          };
    }
}
