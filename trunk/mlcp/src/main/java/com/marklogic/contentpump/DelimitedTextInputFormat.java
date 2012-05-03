package com.marklogic.contentpump;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.marklogic.mapreduce.DocumentURI;

public class DelimitedTextInputFormat extends FileInputFormat<DocumentURI, Text> {

    @Override
    public RecordReader<DocumentURI, Text> createRecordReader(InputSplit arg0,
        TaskAttemptContext arg1) throws IOException, InterruptedException {
        
        return new DelimitedTextReader();
    }
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}
