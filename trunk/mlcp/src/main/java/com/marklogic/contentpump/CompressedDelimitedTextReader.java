package com.marklogic.contentpump;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CompressedDelimitedTextReader extends DelimitedTextReader<Text> {
    private byte[] buf = new byte[65536];
    private ZipInputStream zipIn;
    private ZipEntry currZipEntry;
    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext context)
        throws IOException, InterruptedException {
        initCommonConfigurations(context);
        Path file = ((FileSplit) inSplit).getPath();
        FileSystem fs = file.getFileSystem(context.getConfiguration());
        FSDataInputStream fileIn = fs.open(file);
        zipIn = new ZipInputStream(fileIn);
        initDelimConf(context);
//        CompressionCodec codec = new GzipCodec();
//        InputStream is = codec.createInputStream(fs.open(file));
        
    }
    
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (zipIn == null) {
            hasNext = false;
            return false;
        }
        if(br == null) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            while ((currZipEntry = zipIn.getNextEntry()) != null) {
                System.err.println(currZipEntry.getName());
                if ( currZipEntry.isDirectory() || currZipEntry.getSize() ==0) {
                    continue;
                }
                long size;
                while ((size = zipIn.read(buf, 0, buf.length)) != -1) {
                    baos.write(buf, 0, (int) size);
                }
                br =new BufferedReader( new InputStreamReader(new ByteArrayInputStream(baos.toByteArray())));
                break;
            }
        }
        return super.nextKeyValue();
    }
        
}
