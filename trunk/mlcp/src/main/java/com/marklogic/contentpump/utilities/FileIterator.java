/*
 * Copyright 2003-2014 MarkLogic Corporation
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
package com.marklogic.contentpump.utilities;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;

import com.marklogic.contentpump.FileAndDirectoryInputFormat;

/**
 * A Iterator that returns a FileSplit per file, excluding directories
 * 
 * @author ali
 * 
 */
public class FileIterator implements Iterator<FileSplit> {
    public static final Log LOG = LogFactory.getLog(FileIterator.class);
    protected Iterator<FileSplit> iterator;
    protected List<FileSplit> fileDirSplits;
    protected List<FileSplit> expandedFileSplits;
    protected PathFilter inputFilter;
    protected Configuration conf;

    public FileIterator(Iterator<FileSplit> iterator,
        TaskAttemptContext context) {
        this.iterator = iterator;
        conf = context.getConfiguration();
        fileDirSplits = new LinkedList<FileSplit>();
        PathFilter jobFilter = getInputPathFilter();
        List<PathFilter> filters = new ArrayList<PathFilter>();
        filters.add(FileAndDirectoryInputFormat.hiddenFileFilter);
        if (jobFilter != null) {
            filters.add(jobFilter);
        }
        inputFilter = new FileAndDirectoryInputFormat.MultiPathFilter(filters);
    }

    public FileIterator(FileSplit inSplit, TaskAttemptContext context) {
        conf = context.getConfiguration();
        fileDirSplits = new LinkedList<FileSplit>();
        LinkedList<FileSplit> src = new LinkedList<FileSplit>();
        src.add(inSplit);
        iterator = src.iterator();
        PathFilter jobFilter = getInputPathFilter();
        List<PathFilter> filters = new ArrayList<PathFilter>();
        filters.add(FileAndDirectoryInputFormat.hiddenFileFilter);
        if (jobFilter != null) {
            filters.add(jobFilter);
        }
        inputFilter = new FileAndDirectoryInputFormat.MultiPathFilter(filters);
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext() || !fileDirSplits.isEmpty();
    }

    @Override
    public FileSplit next() {
        while (iterator.hasNext() || !fileDirSplits.isEmpty()) {
            try {
                if (iterator.hasNext()) {
                    FileSplit split = iterator.next();
                    Path file = ((FileSplit) split).getPath();

                    FileSystem fs = file.getFileSystem(conf);

                    FileStatus status = fs.getFileStatus(file);
                    if (status.isDir()) {
                        FileStatus[] children = fs.listStatus(
                            status.getPath(), inputFilter);
                        for (FileStatus stat : children) {
                            FileSplit child = new FileSplit(stat.getPath(), 0,
                                stat.getLen(), null);
                            fileDirSplits.add(child);
                        }
                    } else
                        return split;

                } else if (!fileDirSplits.isEmpty()) {
                    FileSplit split = (FileSplit) fileDirSplits.remove(0);
                    Path file = split.getPath();
                    FileSystem fs = file.getFileSystem(conf);
                    FileStatus status = fs.getFileStatus(file);

                    if (!status.isDir()) {
                        return split;
                    }
                    FileStatus[] children = fs.listStatus(status.getPath(),
                        inputFilter);

                    List<FileSplit> expdFileSpts = new LinkedList<FileSplit>();
                    for (FileStatus stat : children) {
                        FileSplit child = new FileSplit(stat.getPath(), 0,
                            stat.getLen(), null);
                        expdFileSpts.add(child);
                    }
                    iterator = expdFileSpts.iterator();
                    continue;
                }
            } catch (IOException e) {
                LOG.error("Invalid next file", e);
            }
        }
        return null;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    protected PathFilter getInputPathFilter() {
        Class<?> filterClass = conf.getClass("mapred.input.pathFilter.class",
            null, PathFilter.class);
        return (filterClass != null) ? (PathFilter) ReflectionUtils
            .newInstance(filterClass, conf) : null;
    }

}
