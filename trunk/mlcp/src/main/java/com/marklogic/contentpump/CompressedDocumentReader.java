/*******************************************************************************
 * Copyright 2003-2012 MarkLogic Corporation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.marklogic.contentpump;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * RecordReader for CompressedDocumentInputFormat.
 * 
 * @author ali
 * 
 * @param <VALUEIN>
 */
public class CompressedDocumentReader<VALUEIN> extends
    AbstractRecordReader<VALUEIN> {
    private InputStream zipIn;
    private byte[] buf = new byte[65536];
    private boolean hasNext = true;
    private CompressionCodecEnum codec;

    public CompressedDocumentReader() {

    }

    @Override
    public void close() throws IOException {
        if (zipIn != null) {
            zipIn.close();
        }
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return hasNext ? 0 : 1;
    }

    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext context)
        throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Path file = ((FileSplit) inSplit).getPath();
        initCommonConfigurations(conf, file);
        FileSystem fs = file.getFileSystem(context.getConfiguration());
        FSDataInputStream fileIn = fs.open(file);

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
            String filename = file.getName();
            setKey(filename.substring(0, filename.lastIndexOf('.')));
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (zipIn == null) {
            hasNext = false;
            return false;
        }
        if (codec.equals(CompressionCodecEnum.ZIP)) {
            ZipEntry zipEntry;
            ZipInputStream zis = (ZipInputStream) zipIn;
            while ((zipEntry = zis.getNextEntry()) != null) {
                if (zipEntry != null) {
                    setKey(zipEntry.getName());
                    readFromStream();
                    return true;
                }
            }
        } else if (codec.equals(CompressionCodecEnum.GZIP)) {
            readFromStream();
            zipIn.close();
            zipIn = null;
            hasNext = false;
            return true;
        }
        hasNext = false;
        return false;
    }

    @SuppressWarnings("unchecked")
    private void readFromStream() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        long size;
        while ((size = zipIn.read(buf, 0, buf.length)) != -1) {
            baos.write(buf, 0, (int) size);
        }
        if (value instanceof Text) {
            ((Text) value).set(baos.toString());
        } else if (value instanceof BytesWritable) {
            ((BytesWritable) value).set(baos.toByteArray(), 0, baos.size());
        } else {
            LOG.error("Unexpected type: " + value.getClass()); 
            key = null;
        }
        baos.close();
    }

}
