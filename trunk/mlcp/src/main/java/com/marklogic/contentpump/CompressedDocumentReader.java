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

import com.marklogic.mapreduce.DocumentURI;

/**
 * RecordReader for CompressedDocumentInputFormat.
 * 
 * @author ali
 *
 * @param <VALUEIN>
 */
public class CompressedDocumentReader<VALUEIN> extends
        AbstractRecordReader<VALUEIN> {
    private ZipInputStream zipIn;
    private byte[] buf = new byte[65536];
    private boolean hasNext = true;
    
    public CompressedDocumentReader(){
        
    }
    @Override
    public void close() throws IOException {
        if (zipIn != null) {
            zipIn.close();
        }
    }

    @Override
    public DocumentURI getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public VALUEIN getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return hasNext ? 0 : 1;
    }
    
    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {
        initCommonConfigurations(context);
        Path file = ((FileSplit) inSplit).getPath();
        FileSystem fs = file.getFileSystem(context.getConfiguration());
        FSDataInputStream fileIn = fs.open(file);
        zipIn = new ZipInputStream(fileIn);

        Configuration conf = context.getConfiguration();
        // TODO: support codec
        String codec = conf.get(ConfigConstants.CONF_INPUT_COMPRESSION_CODEC);

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (zipIn == null) {
            hasNext = false;
            return false;
        }
        
        ZipEntry zipEntry;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        while ((zipEntry = zipIn.getNextEntry()) != null) {
            if (zipEntry.isDirectory())
                continue;
            if (zipEntry != null) {
                setKey(zipEntry.getName());
                long size;
                while ((size = zipIn.read(buf, 0, buf.length)) != -1) {
                    baos.write(buf, 0, (int) size);
                }
                if (value instanceof Text) {
                    ((Text) value).set(baos.toString());
                } else if (value instanceof BytesWritable) {
                    ((BytesWritable) value).set(baos.toByteArray(), 0,
                        baos.size());
                }
                baos.close();
                return true;
            }
        }

        baos.close();
        hasNext = false;
        return false;
    }

}
