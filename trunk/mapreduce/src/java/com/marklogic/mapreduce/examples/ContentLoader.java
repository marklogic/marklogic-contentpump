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
package com.marklogic.mapreduce.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.GenericOptionsParser;

import com.marklogic.mapreduce.ContentOutputFormat;
import com.marklogic.mapreduce.DocumentURI;

/**
 * Load documents from HDFS into MarkLogicServer.  Used with config file 
 * conf/marklogic-textin-docout.xml.
 */
public class ContentLoader {

    public static class ContentMapper 
    extends Mapper<Text, Text, DocumentURI, Text> {
        
        private DocumentURI uri = new DocumentURI();
        
        public void map(Text fileName, Text fileContent, Context context) 
        throws IOException, InterruptedException {
            uri.setUri(fileName.toString());
            context.write(uri, fileContent);
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length < 2) {
            System.err.println("Usage: ContentLoader configFile inputDir");
            System.exit(2);
        }
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        Job job = Job.getInstance(conf, "content loader");
        job.setJarByClass(ContentLoader.class);
        job.setInputFormatClass(ContentInputFormat.class);
        job.setMapperClass(ContentMapper.class);
        job.setMapOutputKeyClass(DocumentURI.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(ContentOutputFormat.class);
        
        ContentInputFormat.setInputPaths(job, new Path(otherArgs[1]));

        conf = job.getConfiguration();
        conf.addResource(otherArgs[0]);
         
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class ContentInputFormat extends FileInputFormat<Text, Text> {

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
    
    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split,
            TaskAttemptContext context) throws IOException, InterruptedException {
        return new FileReader();
    }    
}

class FileReader extends RecordReader<Text, Text> {

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
