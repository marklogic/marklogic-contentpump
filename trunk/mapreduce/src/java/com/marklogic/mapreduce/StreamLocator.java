/*
 * Copyright (c) 2003-2012 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Writable used to describe how to create and read from an input stream.
 * 
 * @author jchen
 */
public class StreamLocator implements Writable {
    private Path path;
    private CompressionCodec codec;
    
    public StreamLocator() {
    }
    
    public StreamLocator(Path path, CompressionCodec codec) {
        this.path = path;
        this.codec = codec;
    }

    public Path getPath() {
        return path;
    }

    public void setPath(Path path) {
        this.path = path;
    }

    public CompressionCodec getCodec() {
        return codec;
    }

    public void setCodec(CompressionCodec codec) {
        this.codec = codec;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        path = new Path(Text.readString(in));
        codec = WritableUtils.readEnum(in, CompressionCodec.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, path.toString());
        WritableUtils.writeEnum(out, codec);
    }
    
    @Override
    public String toString() {
        return "path: " + path + ", codec: " + codec;
    }
}
