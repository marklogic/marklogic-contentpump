package com.marklogic.contentpump;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.marklogic.mapreduce.DocumentURI;

public class BinaryDocumentMapper extends
        Mapper<DocumentURI, BytesWritable, DocumentURI, BytesWritable> {
    public void map(DocumentURI uri, BytesWritable fileContent, Context context) 
    throws IOException, InterruptedException {
        context.write(uri, fileContent);
    }
}
