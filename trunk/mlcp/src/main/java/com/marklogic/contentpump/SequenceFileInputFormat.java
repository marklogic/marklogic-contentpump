package com.marklogic.contentpump;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.marklogic.mapreduce.DocumentURI;


public class SequenceFileInputFormat <VALUE> extends FileInputFormat<DocumentURI, VALUE> {

    @Override
    public RecordReader<DocumentURI, VALUE> createRecordReader(
        InputSplit arg0, TaskAttemptContext arg1) throws IOException,
        InterruptedException {
        return new SequenceFileReader<VALUE>();
    }
    
//    @Override
//    protected boolean isSplitable(JobContext context, Path filename) {
//        return false;
//    }

}
