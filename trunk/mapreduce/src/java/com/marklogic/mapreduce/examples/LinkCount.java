/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.marklogic.mapreduce.MarkLogicRecord;
import com.marklogic.mapreduce.NodeInputFormat;
import com.marklogic.mapreduce.NodePath;

public class LinkCount {
	public static class RefMapper 
	extends Mapper<NodePath, MarkLogicRecord, Text, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		private Text refURI = new Text();
		
		public void map(NodePath key, MarkLogicRecord value, Context context
		) throws IOException, InterruptedException {
			refURI.set(value.toString());
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
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: OutputTestMR configFile outputDir");
			System.exit(2);
		}

		Job job = new Job(conf);
		job.setJarByClass(LinkCount.class);
		job.setMapperClass(RefMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setInputFormatClass(NodeInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		conf = job.getConfiguration();
		conf.addResource(otherArgs[0]);
	
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

