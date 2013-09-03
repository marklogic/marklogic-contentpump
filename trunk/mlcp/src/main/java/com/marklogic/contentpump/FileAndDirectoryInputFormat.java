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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;

/**
 * An InputFormat that flattens directories and returns file-only splits.
 * 
 * @author jchen
 *
 */
public abstract class FileAndDirectoryInputFormat<K, V> extends
FileInputFormat<K, V> {
	/**
	 * threshold of expanded splits: 1 million
	 */
    private static int SPLIT_COUNT_LIMIT = 1000000;
    // add my own hiddenFileFilter since the one defined in FileInputFormat
    // is not accessible.
    public static final PathFilter hiddenFileFilter = new PathFilter() {
        public boolean accept(Path p) {
            String name = p.getName();
            return !name.startsWith("_") && !name.startsWith(".");
        }
    };

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> splits;
        Configuration conf = job.getConfiguration();
        try {
            splits = super.getSplits(job);
        } catch (InvalidInputException ex) {
            String inPath = conf.get("mapred.input.dir");
            String pattern = conf.get(ConfigConstants.CONF_INPUT_FILE_PATTERN,
                    ".*");
            throw new IOException(
                "No input files found with the specified input path " + inPath
                + " and input file pattern " + pattern, ex);
        }        
        
        // flatten directories until reaching FILE_SPLIT_COUNT_LIMIT
        
        PathFilter jobFilter = getInputPathFilter(job);
        List<PathFilter> filters = new ArrayList<PathFilter>();
        filters.add(hiddenFileFilter);
        if (jobFilter != null) {
            filters.add(jobFilter);
        }
        PathFilter inputFilter = new MultiPathFilter(filters);
        // take a second pass of the splits generated to extract files from 
        // directories
        int count = 0;
        while (count < splits.size() && splits.size() < SPLIT_COUNT_LIMIT) {
            FileSplit split = (FileSplit) splits.get(count);
            Path file = split.getPath();
            FileSystem fs = file.getFileSystem(conf);
            FileStatus status = fs.getFileStatus(file);
            if (status.isDir()) {
                FileStatus[] children = fs.listStatus(file, inputFilter);
                if(children.length + count < SPLIT_COUNT_LIMIT) {
                    splits.remove(count);
                    for (FileStatus stat : children) {
                        FileSplit child = new FileSplit(stat.getPath(), 0, 
                                        stat.getLen(), null);
                        splits.add(child);
                    }
                } else {
                    count++;
                }
            } else {
                count++;
            }
        }
        return splits;
    }
    
    /**
     * Proxy PathFilter that accepts a path only if all filters given in the
     * constructor do. Used by the listPaths() to apply the built-in
     * hiddenFileFilter together with a user provided one (if any).
     */
    public static class MultiPathFilter implements PathFilter {
        private List<PathFilter> filters;

        public MultiPathFilter(List<PathFilter> filters) {
            this.filters = filters;
        }

        public boolean accept(Path path) {
            for (PathFilter filter : filters) {
                if (!filter.accept(path)) {
                    return false;
                }
            }
            return true;
        }
    }
}
