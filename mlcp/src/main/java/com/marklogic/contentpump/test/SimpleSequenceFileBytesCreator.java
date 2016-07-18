package com.marklogic.contentpump.test;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;

import com.marklogic.contentpump.examples.SimpleSequenceFileKey;
import com.marklogic.contentpump.examples.SimpleSequenceFileValue;
import com.marklogic.mapreduce.DocumentURI;

public class SimpleSequenceFileBytesCreator {
    public static void main(String args[]) throws IOException {
        System.out.println("Sequence File Creator");
        String uri = args[0]; // output sequence file name

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);
        SequenceFile.Writer writer = null;
        SimpleSequenceFileKey key = new SimpleSequenceFileKey();
        SimpleSequenceFileValue<BytesWritable> value = new SimpleSequenceFileValue<BytesWritable>();
        try {
            BytesWritable bw = new BytesWritable();
            byte byteArray[] = {2,3,4};
            bw.set(byteArray, 0, byteArray.length);
            
            writer = SequenceFile.createWriter(fs, conf, path, key.getClass(),
                value.getClass());
            
            key.setDocumentURI(new DocumentURI("ABC"));
            value.setValue(bw);
            writer.append(key, value);
            System.err.println(key.getDocumentURI().getUri() + value);
        } finally {
            IOUtils.closeStream(writer);
        }
    }
}