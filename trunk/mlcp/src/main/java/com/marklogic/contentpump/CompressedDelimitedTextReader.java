package com.marklogic.contentpump;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CompressedDelimitedTextReader extends DelimitedTextReader<Text> {
    private byte[] buf = new byte[65536];
    private InputStream zipIn;
    private ZipEntry currZipEntry;
    private CompressionCodecEnum codec;

    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext context)
        throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Path file = ((FileSplit) inSplit).getPath();
        initCommonConfigurations(conf, file);
        FileSystem fs = file.getFileSystem(context.getConfiguration());
        FSDataInputStream fileIn = fs.open(file);

        initDelimConf(conf);

        String codecString = conf.get(
            ConfigConstants.CONF_INPUT_COMPRESSION_CODEC,
            CompressionCodecEnum.ZIP.toString());
        if (codecString.equalsIgnoreCase(CompressionCodecEnum.ZIP.toString())) {
            zipIn = new ZipInputStream(fileIn);
            codec = CompressionCodecEnum.ZIP;
        } else if (codecString.equalsIgnoreCase(CompressionCodecEnum.GZIP
            .toString())) {
            zipIn = new GZIPInputStream(fileIn);
            codec = CompressionCodecEnum.GZIP;
        }

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (zipIn == null) {
            hasNext = false;
            return false;
        }
        if (br == null) {
            if (codec.equals(CompressionCodecEnum.ZIP)) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ZipInputStream zis = (ZipInputStream) zipIn;
                while ((currZipEntry = zis.getNextEntry()) != null) {
                    System.err.println(currZipEntry.getName());
                    if (currZipEntry.isDirectory()
                        || currZipEntry.getSize() == 0) {
                        continue;
                    }
                    long size;
                    while ((size = zis.read(buf, 0, buf.length)) != -1) {
                        baos.write(buf, 0, (int) size);
                    }
                    br = new BufferedReader(new InputStreamReader(
                        new ByteArrayInputStream(baos.toByteArray())));
                    break;
                }
            } else if (codec.equals(CompressionCodecEnum.GZIP)) {
                br = new BufferedReader(new InputStreamReader(zipIn));
            }
        }
        return super.nextKeyValue();
    }

}
