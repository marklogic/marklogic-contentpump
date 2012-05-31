/*
 * Copyright (c) 2003-2012 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * This is just a wrapper of Text, meant to be differentiated from Text by name
 * only to represent XML in serialized form.
 * 
 * @author jchen
 */
public class SerializedXML implements Writable {
    private Text content;
    
    public SerializedXML(){
    }
    
    public SerializedXML(Text content) {
        this.content = content;
    }
    
    public Text getContent() {
        return content;
    }
    
    public void setContent(Text content) {
        this.content = content;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        content.readFields(in);     
    }

    @Override
    public void write(DataOutput out) throws IOException {
        content.write(out);      
    }
}
