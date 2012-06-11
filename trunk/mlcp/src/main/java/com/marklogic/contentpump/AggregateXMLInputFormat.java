/*
 * Copyright 2003-2012 MarkLogic Corporation
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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.marklogic.mapreduce.DocumentURI;

public class AggregateXMLInputFormat extends
    FileInputFormat<DocumentURI, Text> {

    @Override
    public RecordReader<DocumentURI, Text> createRecordReader(InputSplit arg0,
        TaskAttemptContext arg1) throws IOException, InterruptedException {
        return new AggregateXMLReader<Text>();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
    
    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> splits = super.getSplits(job);
        Configuration conf = job.getConfiguration();
        
        // take a second pass of the splits generated to extract files from 
        // directories
        int count = 0;
        while (count < splits.size()) {
            FileSplit split = (FileSplit) splits.get(count);
            Path file = split.getPath();
            FileSystem fs = file.getFileSystem(conf);
            FileStatus status = fs.getFileStatus(file);
            if (status.isDir()) {
                splits.remove(count);
                for (FileStatus stat: fs.listStatus(status.getPath())) {
                    FileSplit child = new FileSplit(stat.getPath(), 0, 
                                    stat.getLen(), null);
                    splits.add(child);
                }
            } else {
                count++;
            }
        }
        return splits;
    }
}
