package com.marklogic.contentpump.test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;

import com.marklogic.contentpump.examples.SimpleSequenceFileKey;
import com.marklogic.contentpump.examples.SimpleSequenceFileValue;
import com.marklogic.mapreduce.DocumentURI;

public class SimpleSequenceFileLargeBinaryCreator {
    public static void main(String args[]) throws IOException {
        System.out.println("Sequence File Creator");
        String uri = args[0]; // output sequence file name
        String binaryfile = args[1]; // large binary file >64MB 
        
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);
        SequenceFile.Writer writer = null;
        SimpleSequenceFileKey key = new SimpleSequenceFileKey();
        SimpleSequenceFileValue<BytesWritable> value = new SimpleSequenceFileValue<BytesWritable>();
        try {
            BytesWritable bw = new BytesWritable();
            Path file = new Path(binaryfile);
            FileSystem fs1 = file.getFileSystem(conf);
            FSDataInputStream fileIn = fs1.open(file);

            byte[] buf = new byte[65535];
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            long size;
            while ((size = fileIn.read(buf, 0, buf.length)) != -1) {
                baos.write(buf, 0, (int) size);
            }
            bw.set(baos.toByteArray(), 0, baos.size());
            
            writer = SequenceFile.createWriter(fs, conf, path, key.getClass(),
                value.getClass());
            
            key.setDocumentURI(new DocumentURI("Large"));
            value.setValue(bw);
            writer.append(key, value);
            System.err.println(key.getDocumentURI().getUri() + " loaded.");
        } finally {
            IOUtils.closeStream(writer);
        }
    }
}