package com.marklogic.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Captures any type of MarkLogic documents.
 *  
 * @author jchen
 *
 */
public interface MarkLogicDocument extends Writable {

    public abstract ContentType getContentType();

    public abstract Text getContentAsText();

    public abstract byte[] getContentAsByteArray();

    public abstract MarkLogicNode getContentAsMarkLogicNode();
    
    public abstract String getContentAsString() 
    throws UnsupportedEncodingException;

    public abstract void readFields(DataInput in) throws IOException;

    public abstract void write(DataOutput out) throws IOException;
}