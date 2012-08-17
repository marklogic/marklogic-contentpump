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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class DelimitedTextReader<VALUEIN> extends
    ImportRecordReader<VALUEIN> {
    public static final Log LOG = LogFactory.getLog(DelimitedTextReader.class);
    protected String[] fields;
    protected String DELIM;
    protected static String ROOT_START = "<root>";
    protected static String ROOT_END = "</root>";
    protected BufferedReader br;
    protected boolean hasNext = true;
    protected String idName;
    protected long fileLen = Long.MAX_VALUE;
    protected long bytesRead;
    @Override
    public void close() throws IOException {
        if (br != null) {
            br.close();
        }
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return bytesRead/fileLen;
    }

    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext context)
        throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Path file = ((FileSplit) inSplit).getPath();
        initCommonConfigurations(conf, file);
        configFileNameAsCollection(conf, file);
        FileSystem fs = file.getFileSystem(context.getConfiguration());
        FSDataInputStream fileIn = fs.open(file);
        br = new BufferedReader(new InputStreamReader(fileIn));
        fileLen = inSplit.getLength();
        initDelimConf(conf);
    }

    protected void initDelimConf(Configuration conf) {
        if (DELIM == null) {
            DELIM = conf.get(ConfigConstants.CONF_DELIMITER,
                ConfigConstants.DEFAULT_DELIMITER);
        }
        if( DELIM.length() == 1){
            DELIM = "\\" + DELIM;
        } else {
            LOG.error("Incorrect delimitor: " + DELIM);
        }
        idName = conf.get(ConfigConstants.CONF_DELIMITED_URI_ID, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (br == null) {
            return false;
        }
        String line = br.readLine();
        
        // skip empty lines
        while (line != null) {
            bytesRead += line.getBytes().length;
            if (!"".equals(line.trim())) {
                break;
            }
            line = br.readLine();
        }
        if (line == null) {
            bytesRead = fileLen;
            return false;
        }
        if (fields == null) {
            fields = line.split(DELIM);
            boolean found = false;
            for (int i = 0; i < fields.length; i++) {
                if (i == 0 && idName == null || fields[i].equals(idName)) {
                    idName = fields[i];
                    found = true;
                    break;
                }
            }
            if (found == false) {
                // idname doesn't match any columns
                LOG.error("delimited_uri_id doesn't match any column");
                throw new IOException(
                    "delimited_uri_id doesn't match any column");
            }
            line = br.readLine();
            
            // skip empty lines
            while (line != null) {
                bytesRead += line.getBytes().length;
                if (!"".equals(line.trim())) {
                    break;
                }
                line = br.readLine();
            }
            if (line == null) {
                bytesRead = fileLen;
                return false;
            } 
        }

        String[] values = line.split(DELIM);
        if (values.length != fields.length) {
            LOG.error(line + " is inconsistent with column definition");
            return true;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(ROOT_START);
        for (int i = 0; i < fields.length; i++) {
            if (idName.equals(fields[i])) {
                if (values[i] == null || values[i].trim().equals("")) {
                    LOG.error(line + ":column used for uri_id is empty");
                    //clear the key of previous record 
                    key = null;
                    return true;
                }
                String uri = getEncodedURI(values[i].trim());
                if (uri != null) {
                    setKey(uri);
                } else {
                    key = null;
                    return true;
                }
            }
            sb.append("<").append(fields[i]).append(">");
            sb.append(values[i]);
            sb.append("</").append(fields[i]).append(">");
        }
        sb.append(ROOT_END);
        if (value instanceof Text) {
            ((Text) value).set(sb.toString());
        } else if (value instanceof ContentWithFileNameWritable) {
            VALUEIN realValue = ((ContentWithFileNameWritable<VALUEIN>) value)
                .getValue();
            if (realValue instanceof Text) {
                ((Text) realValue).set(sb.toString());
            } else {
                LOG.error("Expects Text in delimited text");
                key = null;
            }
        } else {
            LOG.error("Expects Text in delimited text");
            key = null;
        }
        return true;
    }
}
