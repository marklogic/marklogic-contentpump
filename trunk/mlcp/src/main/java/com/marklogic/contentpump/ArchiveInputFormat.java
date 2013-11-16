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
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.DocumentURI;
/**
 * FileInputFormat for reading archive.
 * @author ali
 *
 */
public class ArchiveInputFormat extends
    FileAndDirectoryInputFormat<DocumentURI, DatabaseDocumentWithMeta> {
    public static final Log LOG = LogFactory.getLog(ArchiveInputFormat.class);
    private static String EXTENSION = ".zip";
    
    @Override
    public RecordReader<DocumentURI, DatabaseDocumentWithMeta> createRecordReader(InputSplit split,
        TaskAttemptContext context) throws IOException, InterruptedException {
        return new ArchiveRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> splits = super.getSplits(job);
        Iterator<InputSplit> iter = splits.iterator();
        while(iter.hasNext()) {
            InputSplit s = iter.next();
            Path file = ((FileSplit) s).getPath();
            String zipfile = file.toUri().getPath();
            if(LOG.isDebugEnabled()) {
                LOG.debug("Zip file name: " + zipfile);
            }
            int index = file.toUri().getPath().lastIndexOf(EXTENSION);
            if (index == -1) {
                throw new IOException("Archive file should have suffix .zip");
            }
            String subStr = file.toUri().getPath().substring(0, index);
            index = subStr.lastIndexOf('-');
            if (index == -1) {
                throw new IOException("Not type information in Archive name");
            }
            String typeStr = subStr.substring(index + 1, subStr.length());
            try {
                ContentType.valueOf(typeStr);
            } catch (IllegalArgumentException ex) {
                LOG.warn("Not a valid archive: " + zipfile);
                iter.remove();
            }
        }
        return splits;
    }
    
    
}
