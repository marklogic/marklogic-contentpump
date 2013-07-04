/*
 * Copyright 2003-2013 MarkLogic Corporation
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.marklogic.contentpump.utilities.FileIterator;
import com.marklogic.mapreduce.MarkLogicConstants;

/**
 * RecordReader for CombineDocumentInputFormat.
 * 
 * @author jchen
 *
 * @param <VALUEIN>
 */
public class CombineDocumentReader<VALUEIN> 
extends ImportRecordReader<VALUEIN> {
    public static final Log LOG = LogFactory
        .getLog(CombineDocumentReader.class);
    protected long bytesRead;
    protected long bytesTotal;
    protected TaskAttemptContext context;
    protected int batchSize;
    
    public CombineDocumentReader() {     
    }

    @Override
    public void close() throws IOException {
    }

    /**
     * progress becomes inaccurate if #files exceeds the FILE_SPLIT_COUNT_LIMIT
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return bytesRead / (float)bytesTotal;
    }

    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext context)
    throws IOException, InterruptedException {
        initConfig(context);

        iterator = new FileIterator(((CombineDocumentSplit) inSplit)
            .getSplits().iterator(), context);
        bytesTotal = inSplit.getLength();
        this.context = context;
        batchSize = conf.getInt(MarkLogicConstants.BATCH_SIZE, 
                        MarkLogicConstants.DEFAULT_BATCH_SIZE);
    }

    
    @SuppressWarnings({ "unchecked" })
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        while (iterator.hasNext()) {
            FileSplit split = iterator.next();
            Path file = split.getPath();
            FileSystem fs = file.getFileSystem(context.getConfiguration());
            FSDataInputStream fileIn = fs.open(file);
            byte[] buf = new byte[(int)split.getLength()];
            try {
                String uri = makeURIFromPath(file);
                if (uri != null) {
                    setKey(uri);
                } else {
                    key = null;
                    return true;
                }
                fileIn.readFully(buf);
                if (value instanceof Text) {
                    if (encoding == null) {
                        ((Text) value).set(new String(buf));
                    } else {
                        ((Text) value).set(new String(buf, encoding));
                    }
                } else if (value instanceof BytesWritable) {
                    if (batchSize > 1) {
                        // Copy data since XCC won't do it when Content is 
                        // created.
                        value = (VALUEIN) new BytesWritable(buf);
                    } else {
                        ((BytesWritable) value).set(buf, 0, buf.length);
                    }
                } else {
                    LOG.error("Unexpected type: " + value.getClass());
                    key = null;
                    return true;
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
