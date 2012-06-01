/*
 * Copyright (c) 2003-2012 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.types.ValueType;
import com.marklogic.xcc.types.XdmBinary;

/**
 * Captures any type of MarkLogic documents.
 *  
 * @author jchen
 *
 */
public class MarkLogicDocument implements Writable {
    private byte[] content;
    private ContentType contentType;
    
    public ContentType getContentType() {
        return contentType;
    }
    
    public Text getContentAsText() {
        if (contentType == ContentType.XML || 
            contentType == ContentType.TEXT) {
            return new Text(content);         
        }
        throw new UnsupportedOperationException(
        "Cannot convert binary data to Text.");
    }
    
    public byte[] getContentAsByteArray() {
        return content;
    }
    
    public MarkLogicNode getContentAsMarkLogicNode() {
        if (contentType == ContentType.XML || 
            contentType == ContentType.TEXT) {
            return new MarkLogicNode(getContentAsText().toString(), 
                            contentType);      
        }
        throw new UnsupportedOperationException(
                            "Cannot convert binary data to Text.");        
    }
    
    public void set(ResultItem item) {
        if (item.getValueType() == ValueType.DOCUMENT) {
            content = item.asString().getBytes();
            contentType = ContentType.XML;
        } else if (item.getValueType() == ValueType.TEXT) {
            content = item.asString().getBytes();
            contentType = ContentType.TEXT;
        } else if (item.getValueType() == ValueType.BINARY) {
            content = ((XdmBinary)item.getItem()).asBinaryData();
            contentType = ContentType.BINARY;
        } else {
            contentType = ContentType.UNKNOWN;
        }
    }
    
    public void setXMLFromBytes(byte[] bytes) {
        content = bytes;
        contentType = ContentType.XML;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int ordinal = in.readInt();
        contentType = ContentType.valueOf(ordinal);
        int length = WritableUtils.readVInt(in);
        content = new byte[length];
        in.readFully(content, 0, length);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(contentType.ordinal());
        WritableUtils.writeVInt(out, content.length);
        out.write(content, 0, content.length);
    }
}
