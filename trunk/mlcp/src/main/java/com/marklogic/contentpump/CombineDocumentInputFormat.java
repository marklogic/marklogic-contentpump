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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.marklogic.mapreduce.DocumentURI;

/**
 * InputFormat for reading documents from file systems.  One document on the
 * file system is one record (key value pair).
 * 
 * @author jchen
 *
 */
public class CombineDocumentInputFormat<VALUE>
extends FileAndDirectoryInputFormat<DocumentURI, VALUE> {
    public static final Log LOG = 
        LogFactory.getLog(CombineDocumentInputFormat.class);
    
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
    
    @Override
    public RecordReader<DocumentURI, VALUE> createRecordReader(InputSplit split,
            TaskAttemptContext context) throws IOException, InterruptedException {
        return new CombineDocumentReader<VALUE>();
    } 
    
    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
        long maxSize = getMaxSplitSize(job);      

        // generate splits
        List<InputSplit> splits = super.getSplits(job);
        List<InputSplit> combinedSplits = new ArrayList<InputSplit>();
        CombineDocumentSplit split = null;
        for (InputSplit file: splits) {
            Path path = ((FileSplit)file).getPath();
            FileSystem fs = path.getFileSystem(job.getConfiguration());
            FileStatus status = fs.getFileStatus(path);
            long length = status.getLen();
            long blockSize = status.getBlockSize();
            long splitSize = computeSplitSize(blockSize, minSize, maxSize);
            if (length != 0) {
                if (split == null) {
                    split = new CombineDocumentSplit();
                }

                try {
                    if (split.getLength() + length < splitSize ||
                        split.getLength() < minSize ) {
                        split.addSplit((FileSplit) file); 
                    } else {
                        combinedSplits.add(split);
                        split = new CombineDocumentSplit();
                        split.addSplit((FileSplit) file);
                    }
                } catch (InterruptedException e) {
                    LOG.error(e);
                    throw new RuntimeException(e);
                }
            } 
        }
        if (split != null) {
            combinedSplits.add(split);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Total # of splits: " + splits.size());
            LOG.debug("Total # of combined splits: " + combinedSplits.size());
        }
        
        return combinedSplits;
    }
}
