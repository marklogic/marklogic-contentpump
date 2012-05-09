/*
 * Copyright 2003-2012 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.contentpump;

import java.io.IOException;
import java.util.Iterator;

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
 * RecordReader for CombineDocumentInputFormat.
 * 
 * @author jchen
 *
 * @param <VALUEIN>
 */
public class CombineDocumentReader<VALUEIN> 
extends AbstractRecordReader<VALUEIN> {
    
    private long bytesRead;
    private long bytesTotal;
    private Iterator<FileSplit> iterator;
    private TaskAttemptContext context;

    public CombineDocumentReader() {     
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public DocumentURI getCurrentKey() 
    throws IOException, InterruptedException {
        return key;
    }

    @Override
    public VALUEIN getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return bytesRead / (float)bytesTotal;
    }

    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext context)
    throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        initCommonConfigurations(conf);
        iterator = ((CombineDocumentSplit)inSplit).getSplits().iterator();
        bytesTotal = inSplit.getLength();
        this.context = context;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (iterator.hasNext()) {
            FileSplit split = iterator.next();
            Path file = split.getPath();
            FileSystem fs = file.getFileSystem(context.getConfiguration());
            FSDataInputStream fileIn = fs.open(file);
            setKey(file.toString());
            byte[] buf = new byte[(int)split.getLength()];
            try {
                fileIn.readFully(buf);
                if (value instanceof Text) {
                    ((Text)value).set(new String(buf));
                } else if (value instanceof BytesWritable) {
                    ((BytesWritable)value).set(buf, 0, buf.length);
                }
                bytesRead += buf.length;
                return true;
            } catch (IOException e) {
                LOG.error(e);
                throw e;
            } finally {
                fileIn.close();
            }
        } else {
            return false;
        }
    }
}
