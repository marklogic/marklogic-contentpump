package com.marklogic.mapreduce.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
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

public class BinaryLoader {
    public static class ContentMapper 
    extends Mapper<Text, BytesWritable, DocumentURI, BytesWritable> {
        
        private DocumentURI uri = new DocumentURI();
        
        public void map(Text fileName, BytesWritable fileContent, Context context) 
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
        
        Job job = Job.getInstance(conf);
        job.setJarByClass(BinaryLoader.class);
        job.setInputFormatClass(BinaryInputFormat.class);
        job.setMapperClass(ContentMapper.class);
        job.setMapOutputKeyClass(DocumentURI.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(ContentOutputFormat.class);
        
        BinaryInputFormat.setInputPaths(job, new Path(otherArgs[1]));

        conf = job.getConfiguration();
        conf.addResource(otherArgs[0]);
         
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class BinaryInputFormat extends FileInputFormat<Text, BytesWritable> {

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
    
    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split,
            TaskAttemptContext context) throws IOException, InterruptedException {
        return new BinaryFileReader();
    }    
}

class BinaryFileReader extends RecordReader<Text, BytesWritable> {

    private Text key = new Text();
    private BytesWritable value = new BytesWritable();
    private long bytesRead;
    private long bytesTotal;
    private boolean hasNext;
    
    public BinaryFileReader() {
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
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
        System.out.println("split length: " + inSplit.getLength());
        try {
            fileIn.readFully(buf);
            value.set(buf, 0, (int) inSplit.getLength());
            System.out.println("value length: " + value.getBytes().length);
            
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
