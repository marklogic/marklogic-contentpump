/*
 * Copyright 2003-2015 MarkLogic Corporation
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
import java.util.List;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVStrategy;
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

import com.marklogic.contentpump.utilities.DelimitedSplit;
import com.marklogic.contentpump.utilities.EncodingUtil;
import com.marklogic.contentpump.utilities.XMLUtil;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.MarkLogicConstants;
import com.marklogic.mapreduce.utilities.TextArrayWritable;
import com.sun.org.apache.xml.internal.utils.XMLChar;

/**
 * InputFormat for delimited text. Each line after metadata(1st line) is a
 * record.
 * 
 * @author ali
 * 
 */
public class DelimitedTextInputFormat extends
FileAndDirectoryInputFormat<DocumentURI, Text> {
    public static final Log LOG = LogFactory
        .getLog(DelimitedTextInputFormat.class);
    @Override
    public RecordReader<DocumentURI, Text> createRecordReader(InputSplit arg0,
        TaskAttemptContext arg1) throws IOException, InterruptedException {
        if(isSplitInput(arg1.getConfiguration())) {
            return new SplitDelimitedTextReader<Text>();
        } else {
            return new DelimitedTextReader<Text>();
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
        List<InputSplit> populatedSplits = new ArrayList<InputSplit>();
        LOG.info(splits.size() + " DelimitedSplits generated");
        Configuration conf = job.getConfiguration();
        char delimiter =0;
        ArrayList<Text> hlist = new ArrayList<Text>();
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

                CSVParser parser = new CSVParser(instream, new CSVStrategy(
                    delimiter, DelimitedTextReader.encapsulator,
                    CSVStrategy.COMMENTS_DISABLED,
                    CSVStrategy.ESCAPE_DISABLED, true, true, false, true));
                String[] header = parser.getLine();
                EncodingUtil.handleBOMUTF8(header, 0);
                for (int i = 0; i < header.length; i++) {
                    // skip empty col in header generated by trailing delimiter
                    if (i == header.length -1 && header[i].trim().equals("")) {
                        header[i] = "";
                        break;
                    }
                    if (!XMLChar.isValidName(header[i])) {
                        header[i] = XMLUtil.getValidName(header[i]);
                    }
                }
                
                hlist.clear();
                for (String s : header) {
                    hlist.add(new Text(s));
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
