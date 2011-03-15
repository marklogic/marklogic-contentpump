package com.marklogic.mapreduce.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

import com.marklogic.mapreduce.DocumentInputFormat;
import com.marklogic.mapreduce.DocumentOutputFormat;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.MarkLogicRecord;

public class DocInDocOut {
	public static class RecordMapper 
	extends Mapper<DocumentURI, MarkLogicRecord, DocumentURI, MarkLogicRecord>{
		public void map(DocumentURI key, MarkLogicRecord value, Context context
		) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}
	
	public static class RecordReducer
	extends Reducer<DocumentURI, MarkLogicRecord, DocumentURI, MarkLogicRecord> {
		public void reduce(DocumentURI key, Iterable<MarkLogicRecord> values, 
				Context context
				) throws IOException, InterruptedException {		
			for (MarkLogicRecord val : values) {
				context.write(key, val);
			}
		}
	}
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 1) {
			System.err.println("Usage: OutputTestMR configFile");
			System.exit(2);
		}

		Job job = new Job(conf);
		job.setJarByClass(DocInDocOut.class);
		job.setMapperClass(RecordMapper.class);
		job.setReducerClass(RecordReducer.class);
		job.setInputFormatClass(DocumentInputFormat.class);
		job.setOutputFormatClass(DocumentOutputFormat.class);
		job.setOutputKeyClass(DocumentURI.class);
	    job.setOutputValueClass(MarkLogicRecord.class);
		
		conf = job.getConfiguration();
		conf.addResource(otherArgs[0]);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}