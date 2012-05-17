package com.marklogic.contentpump.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import com.marklogic.mapreduce.DocumentURI;

public class SimpleSequenceFileCreator {
    public static void main(String args[]) throws Exception {
        System.out.println("Sequence File Creator");
        String uri = args[0]; // output sequence file name
        String filePath = args[1]; // text file to read from; Odd line is key,
                                   // even line is value

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);
        SequenceFile.Writer writer = null;
        SimpleSequenceFileKey key = new SimpleSequenceFileKey();

        BufferedReader buffer = new BufferedReader(new FileReader(filePath));
        String line = null;

        SimpleSequenceFileValue<Text> value = new SimpleSequenceFileValue<Text>();
        try {
            writer = SequenceFile.createWriter(fs, conf, path, key.getClass(),
                value.getClass());
            while ((line = buffer.readLine()) != null) {
                key.setDocumentURI(new DocumentURI(line));
                if ((line = buffer.readLine()) == null) {
                    break;
                }
                value.setValue(new Text(line));
                writer.append(key, value);
            }
        } finally {
            IOUtils.closeStream(writer);
        }
    }
}