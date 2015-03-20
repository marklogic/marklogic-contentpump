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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.marklogic.mapreduce.DocumentInputFormat;
import com.marklogic.mapreduce.DocumentURI;

/**
 * Read binary documents from MarkLogic Server and write the content out to 
 * HDFS.  Can be used with config file conf/marklogic-subbinary.xml, which 
 * demonstrates how to use advanced mode with split range binding.
 */
public class BinaryReader {
    public static class DocMapper 
    extends Mapper<DocumentURI, BytesWritable, DocumentURI, BytesWritable> {
        public static final Log LOG =
            LogFactory.getLog(DocMapper.class);
        public void map(DocumentURI key, BytesWritable value, Context context) 
        throws IOException, InterruptedException {
            if (key != null && value != null) {
                LOG.debug("value length: " + value.getLength());
                context.write(key, value);
            } else {
                LOG.error("key: " + key + ", value: " + value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
      
        if (args.length < 2) {
            System.err.println("Usage: BinaryReader configFile outputDir");
            System.exit(2);
        }
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Job job = new Job(conf);
        job.setJarByClass(BinaryReader.class);
        job.setInputFormatClass(DocumentInputFormat.class);
        job.setMapperClass(DocMapper.class);
        job.setMapOutputKeyClass(DocumentURI.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputFormatClass(BinaryOutputFormat.class);
        job.setOutputKeyClass(DocumentURI.class);
        job.setOutputValueClass(BytesWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        conf = job.getConfiguration();
        conf.addResource(otherArgs[0]);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class BinaryOutputFormat extends FileOutputFormat<DocumentURI, BytesWritable> {

    @Override
    public RecordWriter<DocumentURI, BytesWritable> getRecordWriter(
            TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new BinaryWriter(getOutputPath(context), 
                context.getConfiguration());
    }
}

class BinaryWriter extends RecordWriter<DocumentURI, BytesWritable> {

    Path dir;
    Configuration conf;
    
    public BinaryWriter(Path path, Configuration conf) {
        dir = path;
        this.conf = conf;
    }

    @Override
    public void close(TaskAttemptContext arg0) throws IOException,
            InterruptedException {  
    }

    @Override
    public void write(DocumentURI uri, BytesWritable content)
            throws IOException, InterruptedException {
        String pathStr = dir.getName() + uri.getUri();
        Path path = new Path(pathStr);
        FileSystem fs = path.getFileSystem(conf);
        FSDataOutputStream out = fs.create(path, false);
        System.out.println("writing to: " + path);
        out.write(content.getBytes(), 0, content.getLength());
        out.flush();
        out.close();
    }       
}