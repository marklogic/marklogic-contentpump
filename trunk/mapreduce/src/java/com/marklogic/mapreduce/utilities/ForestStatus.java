package com.marklogic.mapreduce.utilities;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ForestStatus implements Writable {
    private Text hostName;
    private LongWritable docCount;
    private BooleanWritable updatable;
    
    public ForestStatus() {}
    
    public ForestStatus(Text hostName, LongWritable docCount, BooleanWritable updatable) {
        super();
        this.hostName = hostName;
        this.docCount = docCount;
        this.updatable = updatable;
    }

    public LongWritable getDocCount() {
        return docCount;
    }
    
    public Text getHostName() {
        return hostName;
    }

    public BooleanWritable getUpdatable() {
        return updatable;
    }

    public void readFields(DataInput in) throws IOException {
        hostName = new Text();
        docCount = new LongWritable();
        updatable = new BooleanWritable();
        hostName.readFields(in);
        docCount.readFields(in);
        updatable.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        hostName.write(out);
        docCount.write(out);
        updatable.write(out);
    }

}
