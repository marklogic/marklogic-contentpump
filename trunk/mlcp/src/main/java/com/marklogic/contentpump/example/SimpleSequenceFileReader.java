package com.marklogic.contentpump.example;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class SimpleSequenceFileReader {
    public static void main(String args[]) throws Exception {
        System.out.println("Sequence File Reader");
        String uri = args[0]; // Input should be a sequence file
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);

        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(fs, path, conf);
            Writable key = (Writable) ReflectionUtils.newInstance(
                reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(
                reader.getValueClass(), conf);

            long position = reader.getPosition();
            while (reader.next(key, value)) {
                String syncSeen = reader.syncSeen() ? "*" : "";
                System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen,
                    ((SimpleSequenceFileKey) key).getDocumentURI().getUri(),
                    ((SimpleSequenceFileValue) value).getValue());
                position = reader.getPosition();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(reader);
        }
    }
}