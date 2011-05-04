package com.marklogic.mapreduce.examples;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.marklogic.mapreduce.DocumentInputFormat;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.MarkLogicNode;

/**
 * Read documents from MarkLogic Server and write them out to HDFS.  Used with
 * config file conf/marklogic-docin-docout.xml.
 *
 */
public class ContentReader {
	public static class DocMapper 
	extends Mapper<DocumentURI, MarkLogicNode, DocumentURI, MarkLogicNode> {
		public static final Log LOG =
		    LogFactory.getLog(DocMapper.class);
		public void map(DocumentURI key, MarkLogicNode value, Context context) 
		throws IOException, InterruptedException {
			if (key != null && value != null) {
			    context.write(key, value);
			} else {
				LOG.error("key: " + key + ", value: " + value);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 1) {
			System.err.println("Usage: ContentReader configFile outputDir");
			System.exit(2);
		}

		Job job = new Job(conf);
		job.setJarByClass(ContentReader.class);
		job.setInputFormatClass(DocumentInputFormat.class);
		job.setMapperClass(DocMapper.class);
		job.setMapOutputKeyClass(DocumentURI.class);
		job.setMapOutputValueClass(MarkLogicNode.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(DocumentURI.class);
		job.setOutputValueClass(MarkLogicNode.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		conf = job.getConfiguration();
		conf.addResource(otherArgs[0]);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
