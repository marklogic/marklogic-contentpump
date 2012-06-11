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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

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
    private List<FileSplit> files;
    private int count = 0;
    private TaskAttemptContext context;
    private Configuration conf;
    public CombineDocumentReader() {     
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return bytesRead / (float)bytesTotal;
    }

    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext context)
    throws IOException, InterruptedException {
        conf = context.getConfiguration();
        //we don't know the file yet until nextKeyValue()
        initCommonConfigurations(conf, null);
        files = ((CombineDocumentSplit)inSplit).getSplits();
        bytesTotal = inSplit.getLength();
        this.context = context;
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        while (count < files.size()) {
            FileSplit split = files.get(count++);
            Path file = split.getPath();
            FileSystem fs = file.getFileSystem(conf);
            FileStatus status = fs.getFileStatus(file);
            if (status.isDir()) {
                for (FileStatus stat: fs.listStatus(status.getPath())) {
                    FileSplit child = new FileSplit(stat.getPath(), 0, 
                                    stat.getLen(), null);
                    files.add(child);
                }
                continue;
            }
            configFileNameAsCollection(conf, file);
            FSDataInputStream fileIn = fs.open(file);
            setKey(file.toString());
            byte[] buf = new byte[(int)split.getLength()];
            try {
                fileIn.readFully(buf);
                if (value instanceof Text) {
                    ((Text) value).set(new String(buf));
                } else if (value instanceof BytesWritable) {
                    ((BytesWritable) value).set(buf, 0, buf.length);
                } else if (value instanceof ContentWithFileNameWritable) {
                    VALUEIN realValue = ((ContentWithFileNameWritable<VALUEIN>) value)
                        .getValue();
                    if (realValue instanceof Text) {
                        ((Text) realValue).set(new String(buf));
                    } else if (realValue instanceof BytesWritable) {
                        ((BytesWritable) realValue).set(buf, 0, buf.length);
                    }
                }
                bytesRead += buf.length;
                return true;
            } catch (IOException e) {
                LOG.error(e);
                throw e;
            } finally {
                fileIn.close();
            }
        }        
        return false;
    }
}
