package com.marklogic.mapreduce.examples;

import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

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

public class ZipContentLoader {
	public static class ZipContentMapper 
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
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: ContentLoader configFile inputDir");
			System.exit(2);
		}
		
		Job job = new Job(conf);
		job.setJarByClass(ZipContentLoader.class);
		job.setInputFormatClass(ZipContentInputFormat.class);
		job.setMapperClass(ZipContentMapper.class);
		job.setMapOutputKeyClass(DocumentURI.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputFormatClass(ContentOutputFormat.class);
		
		ContentInputFormat.setInputPaths(job, new Path(otherArgs[1]));

		conf = job.getConfiguration();
		conf.addResource(otherArgs[0]);
	 	
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

class ZipContentInputFormat extends FileInputFormat<Text, Text> {

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
	    return false;
	}
	
	@Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split,
            TaskAttemptContext context) throws IOException, InterruptedException {
	    return new ZipContentReader();
    }
	
}

class ZipContentReader extends RecordReader<Text, Text> {

	private Text key = new Text();
	private Text value = new Text();
	private ZipInputStream zipIn;
	private long bytesRead = 0;
	private long bytesTotal;
	private byte[] buf = new byte[65536];
	
	@Override
    public void close() throws IOException {
		if (zipIn != null) {
			zipIn.close();
		}
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
		zipIn = new ZipInputStream(fileIn);
    }

	@Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
	    if (zipIn != null) {
	    	ZipEntry zipEntry;
	    	while ((zipEntry = zipIn.getNextEntry()) != null) {
	    	    if (zipEntry != null && !zipEntry.isDirectory()) {
	    		    key.set(zipEntry.getName());
	    		    StringBuilder entry = new StringBuilder();
	    		    long size;
	    	    	while ((size = zipIn.read(buf, 0, buf.length)) != -1) {
	    	    		entry.append(new String(buf, 0, (int) size));
	    	    	}
	    		    value.set(entry.toString());
	    		    System.out.println(entry);
	    		    return true;
	    	    }
	    	}
	    	return false;
	    }
	    return false;
    }
	
}
