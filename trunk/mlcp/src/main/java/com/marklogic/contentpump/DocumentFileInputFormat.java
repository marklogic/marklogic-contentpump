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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * InputFormat for reading documents from file systems.  Once document on the
 * file system is one record (key value pair).
 * 
 * @author jchen
 *
 */
public class DocumentFileInputFormat extends FileInputFormat<Text, Text> {
    
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
    
    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split,
            TaskAttemptContext context) throws IOException, InterruptedException {
        return new FileReader();
    } 
    
    public static class FileReader extends RecordReader<Text, Text> {

        private Text key = new Text();
        private Text value = new Text();
        private long bytesRead;
        private long bytesTotal;
        private boolean hasNext;
        
        public FileReader() {
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return bytesRead / (float)bytesTotal;
        }

        @Override
        public void initialize(InputSplit inSplit, TaskAttemptContext context)
                throws IOException, InterruptedException {
            bytesTotal = inSplit.getLength();
            Path file = ((FileSplit)inSplit).getPath();
            FileSystem fs = file.getFileSystem(context.getConfiguration());
            FSDataInputStream fileIn = fs.open(file);
            key.set(file.toString());
            byte[] buf = new byte[(int)inSplit.getLength()];
            try {
                fileIn.readFully(buf);
                value.set(buf);
                hasNext = true;    
            } catch (Exception e) {
                hasNext = false;
            } finally {
                fileIn.close();
            }
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (hasNext) {
                hasNext = false;
                return true;
            }
            return false;
        }
        
    }
}
