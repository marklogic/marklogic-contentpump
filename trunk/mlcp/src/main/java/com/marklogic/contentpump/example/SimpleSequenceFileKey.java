package com.marklogic.contentpump.example;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import com.marklogic.contentpump.SequenceFileKey;
import com.marklogic.mapreduce.DocumentURI;

public class SimpleSequenceFileKey implements SequenceFileKey, Writable {
    private DocumentURI uri = new DocumentURI();

    public void setDocumentURI(DocumentURI uri) {
        this.uri = uri;
    }

    @Override
    public DocumentURI getDocumentURI() {
        return uri;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        uri.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        uri.write(out);
    }

}
