/*
 * Copyright (c) 2021 MarkLogic Corporation
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
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.marklogic.contentpump.utilities.CSVParserFormatter;
import com.marklogic.contentpump.utilities.DelimitedSplit;
import com.marklogic.contentpump.utilities.EncodingUtil;
import com.marklogic.mapreduce.DocumentURIWithSourceInfo;
import com.marklogic.mapreduce.MarkLogicConstants;
import com.marklogic.mapreduce.utilities.TextArrayWritable;

/**
 * InputFormat for delimited text. Each line after metadata(1st line) is a
 * record.
 * 
 * @author ali
 * 
 */
public class DelimitedTextInputFormat extends
FileAndDirectoryInputFormat<DocumentURIWithSourceInfo, Text> {
    public static final Log LOG = LogFactory.getLog(
            DelimitedTextInputFormat.class);
    @Override
    public RecordReader<DocumentURIWithSourceInfo, Text> createRecordReader(
            InputSplit split, TaskAttemptContext context) 
            throws IOException, InterruptedException {
        if (isSplitInput(context.getConfiguration())) {
            return new SplitDelimitedTextReader<>();
        } else {
            return new DelimitedTextReader<>();
        }
    }

    
    private boolean isSplitInput(Configuration conf ) {
        return conf.getBoolean(
            ConfigConstants.CONF_SPLIT_INPUT, false);
    }

    public List<InputSplit> getSplits(JobContext job) throws IOException {
        boolean delimSplit = isSplitInput(job.getConfiguration());
        //if delimSplit is true, size of each split is determined by 
        //Math.max(minSize, Math.min(maxSize, blockSize)) in FileInputFormat
        List<InputSplit> splits = super.getSplits(job);
        if (!delimSplit) {
            return splits;
        }

        if (splits.size()>= SPLIT_COUNT_LIMIT) {
            //if #splits > 1 million, there is enough parallelism
            //therefore no point to split
            LOG.warn("Exceeding SPLIT_COUNT_LIMIT, input_split is off:"
                + SPLIT_COUNT_LIMIT);
            DefaultStringifier.store(job.getConfiguration(), false, ConfigConstants.CONF_SPLIT_INPUT);
            return splits;
        }
        // add header info into splits
        List<InputSplit> populatedSplits = new ArrayList<>();
        LOG.info(splits.size() + " DelimitedSplits generated");
        Configuration conf = job.getConfiguration();
        char delimiter =0;
        ArrayList<Text> hlist = new ArrayList<>();
        for (InputSplit file: splits) {
            FileSplit fsplit = ((FileSplit)file);
            Path path = fsplit.getPath();
            FileSystem fs = path.getFileSystem(conf);
            
            if (fsplit.getStart() == 0) {
            // parse the inSplit, get the header
                FSDataInputStream fileIn = fs.open(path);

                String delimStr = conf.get(ConfigConstants.CONF_DELIMITER,
                    ConfigConstants.DEFAULT_DELIMITER);
                if (delimStr.length() == 1) {
                    delimiter = delimStr.charAt(0);
                } else {
                    LOG.error("Incorrect delimitor: " + delimiter
                        + ". Expects single character.");
                }
                String encoding = conf.get(
                    MarkLogicConstants.OUTPUT_CONTENT_ENCODING,
                    MarkLogicConstants.DEFAULT_OUTPUT_CONTENT_ENCODING);
                InputStreamReader instream = new InputStreamReader(fileIn, encoding);
                CSVParser parser = new CSVParser(instream, CSVParserFormatter.
                		getFormat(delimiter, DelimitedTextReader.encapsulator,
                				true, true));
                Iterator<CSVRecord> it = parser.iterator();
                
                String[] header = null;
                if (it.hasNext()) {
                	CSVRecord record = (CSVRecord)it.next();
                	Iterator<String> recordIterator = record.iterator();
                    int recordSize = record.size();
                    header = new String[recordSize];
                    for (int i = 0; i < recordSize; i++) {
                    	if (recordIterator.hasNext()) {
                    		header[i] = (String)recordIterator.next();
                    	} else {
                    		throw new IOException("Record size doesn't match the real size");
                    	}
                    }
                    
                    EncodingUtil.handleBOMUTF8(header, 0);
                    
                    hlist.clear();
                    for (String s : header) {
                        hlist.add(new Text(s));
                    }
                }
                instream.close();
            }
            
            DelimitedSplit ds = new DelimitedSplit(new TextArrayWritable(
                hlist.toArray(new Text[hlist.size()])), path,
                fsplit.getStart(), fsplit.getLength(),
                fsplit.getLocations());
            populatedSplits.add(ds);
        }
        
        return populatedSplits;
    }
    
}
