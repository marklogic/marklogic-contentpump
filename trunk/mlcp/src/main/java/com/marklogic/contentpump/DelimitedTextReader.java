package com.marklogic.contentpump;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.marklogic.mapreduce.DocumentURI;

public class DelimitedTextReader extends RecordReader<DocumentURI, Text> {
    public static final Log LOG = 
        LogFactory.getLog(DelimitedTextReader.class);
    
    protected static String[] fields;
    protected static String DELIM;
    protected static String ROOT_START = "<root>";
    protected static String ROOT_END = "</root>";
    protected BufferedReader br;
    protected DocumentURI key = new DocumentURI();
    protected Text value = new Text();
    protected boolean hasNext = true;
    protected String idName;
    @Override
    public void close() throws IOException {
        br.close();
        
    }

    @Override
    public DocumentURI getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return hasNext == true ? 0 : 1;
    }

    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext context)
        throws IOException, InterruptedException {
        Path file = ((FileSplit) inSplit).getPath();
        FileSystem fs = file.getFileSystem(context.getConfiguration());
        FSDataInputStream fileIn = fs.open(file);
        br = new BufferedReader(new InputStreamReader(fileIn));
        initConf(context);
    }
    
    protected void initConf(TaskAttemptContext context){
        Configuration conf = context.getConfiguration();
        DELIM = conf.get(ConfigConstants.DELIMITER, ConfigConstants.CONF_DEFAULT_DELIMITER);
        idName = conf.get(ConfigConstants.CONF_DELIMITED_URI_ID, null);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(br == null ) {
            return false;
        }
        String line = br.readLine();
        if(line == null) {
            return false;
        }
        if (fields == null) {
            fields = line.split(DELIM);
            for (int i=0; i<fields.length; i++) {
                if(i==0 && idName == null || fields[i].equals(idName)){
                    idName = fields[i];
                    break;
                }
            }
            line = br.readLine();
        }
        
        String[] values = line.split(DELIM);
        StringBuilder sb = new StringBuilder();
        sb.append(ROOT_START);
        for (int i=0; i<fields.length; i++) {
            if(idName.equals(fields[i])) {
                key.setUri(values[i]);
            }
            sb.append("<").append(fields[i]).append(">");
            sb.append(values[i]);
            sb.append("</").append(fields[i]).append(">");
        }
        sb.append(ROOT_END);
        value.set(sb.toString());
        return true;
    }

}
