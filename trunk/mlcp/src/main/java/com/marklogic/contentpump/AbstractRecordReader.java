package com.marklogic.contentpump;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.MarkLogicConstants;

public abstract class AbstractRecordReader<VALUEIN> extends RecordReader<DocumentURI, VALUEIN> {
    public static final Log LOG = 
        LogFactory.getLog(AbstractRecordReader.class);
    protected DocumentURI key = new DocumentURI();
    protected VALUEIN value;
    protected String prefix;
    protected String suffix;
    
    protected void setKey(String uri) {
        StringBuilder sb = new StringBuilder();
        if (prefix != null) {
            sb.append(prefix);
        }
        sb.append(uri);
        if(suffix != null) {
            sb.append(suffix);
        }
        this.key.setUri(sb.toString());
    }
    
    @Override
    public void close() throws IOException {
    }

    @Override
    public DocumentURI getCurrentKey() throws IOException,
        InterruptedException {
        return null;
    }

    @Override
    public VALUEIN getCurrentValue() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }


    @Override
    public void initialize(InputSplit arg0, TaskAttemptContext context)
        throws IOException, InterruptedException {
    }
    
    @SuppressWarnings("unchecked")   
    public void initCommonConfigurations(TaskAttemptContext context){
        Configuration conf = context.getConfiguration();
        prefix = conf.get(ConfigConstants.CONF_OUTPUT_URI_PREFIX);
        suffix = conf.get(ConfigConstants.CONF_OUTPUT_URI_SUFFIX);
        String type = conf.get(MarkLogicConstants.CONTENT_TYPE, 
                MarkLogicConstants.DEFAULT_CONTENT_TYPE);
        ContentType contentType = ContentType.valueOf(type);
        Class<? extends Writable> valueClass = contentType.getWritableClass();
        value = (VALUEIN) ReflectionUtils.newInstance(valueClass, conf);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return false;
    }

}
