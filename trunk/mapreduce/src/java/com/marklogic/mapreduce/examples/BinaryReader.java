package com.marklogic.mapreduce.examples;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

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

        Job job = new Job(conf);
        job.setJarByClass(BinaryReader.class);
        job.setInputFormatClass(DocumentInputFormat.class);
        job.setMapperClass(DocMapper.class);
        job.setMapOutputKeyClass(DocumentURI.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(DocumentURI.class);
        job.setOutputValueClass(BytesWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        conf = job.getConfiguration();
        conf.addResource(args[0]);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
