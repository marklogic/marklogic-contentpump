package com.marklogic.mapreduce.examples;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

import com.marklogic.mapreduce.KeyValueInputFormat;

/**
 * Read link titles as text and their frequencies and write link count summary 
 * to HDFS.  Used with config file conf/marklogic-advanced.xml.
 */
public class LinkCount {
	public static class RefMapper 
	extends Mapper<Text, IntWritable, Text, IntWritable> {
		public static final Log LOG =
		    LogFactory.getLog(RefMapper.class);
		public void map(Text key, IntWritable value, Context context) 
		throws IOException, InterruptedException {
			if (key != null && value != null) {
			    context.write(key, value);
			} else {
				LOG.error("key: " + key + ", value: " + value);
			}
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
		job.setInputFormatClass(KeyValueInputFormat.class);
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

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
