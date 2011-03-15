package com.marklogic.mapreduce.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

import com.marklogic.mapreduce.DocumentOutputFormat;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.MarkLogicRecord;
import com.marklogic.mapreduce.NodeInputFormat;
import com.marklogic.mapreduce.NodePath;

public class NodeInDocOut {
	public static class RecordMapper 
	extends Mapper<NodePath, MarkLogicRecord, DocumentURI, MarkLogicRecord>{
		public void map(NodePath key, MarkLogicRecord value, Context context
		) throws IOException, InterruptedException {
			context.write(new DocumentURI(key.getDocumentUri()), value);
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
		job.setJarByClass(NodeInDocOut.class);
		job.setMapperClass(RecordMapper.class);
		job.setReducerClass(RecordReducer.class);
		job.setInputFormatClass(NodeInputFormat.class);
		job.setOutputFormatClass(DocumentOutputFormat.class);
		job.setOutputKeyClass(DocumentURI.class);
	    job.setOutputValueClass(MarkLogicRecord.class);
		
		conf = job.getConfiguration();
		conf.addResource(otherArgs[0]);
	
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
