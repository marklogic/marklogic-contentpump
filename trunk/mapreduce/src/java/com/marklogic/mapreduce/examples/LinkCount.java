package com.marklogic.mapreduce.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.marklogic.mapreduce.ValueInputFormat;

/**
 * Count the frequency of occurrences of link titles in documents in
 * MarkLogic Server, and write a link count summary to HDFS. 
 * Use with the configuration file conf/marklogic-advanced.xml.
 */
public class LinkCount {
    public static class RefMapper 
    extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text refURI = new Text();

        public void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException {
            refURI.set(value);
            context.write(refURI, one);
        }
    }
    
    public static class IntSumReducer
    extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        
        public void reduce(Text key, Iterable<IntWritable> values, 
                Context context
                ) throws IOException, InterruptedException {        
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 1) {
            System.err.println("Usage: LinkCount configFile outputDir");
            System.exit(2);
        }

        Job job = new Job(conf);
        job.setJarByClass(LinkCount.class);
        job.setInputFormatClass(ValueInputFormat.class);
        job.setMapperClass(RefMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        conf = job.getConfiguration();
        conf.addResource(otherArgs[0]);
        conf.setClass("mapreduce.marklogic.input.valueClass", Text.class, 
                Writable.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
