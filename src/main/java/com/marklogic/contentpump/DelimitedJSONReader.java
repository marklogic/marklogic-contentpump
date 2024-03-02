/*
 * Copyright (c) 2023 MarkLogic Corporation
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
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.marklogic.contentpump.utilities.FileIterator;
import com.marklogic.contentpump.utilities.IdGenerator;

/**
 * Reader for DelimitedJSONInputFormat.
 * 
 * @author mattsun
 *
 * @param <VALUEIN>
 */
public class DelimitedJSONReader<VALUEIN> extends 
    ImportRecordReader<VALUEIN> {
    /* Log */
    public static final Log LOG = LogFactory.getLog(DelimitedJSONReader.class);
    /* Input Handling */
    protected InputStreamReader instream;
    protected FSDataInputStream fileIn;
    // LineNumberReader inherits from BufferedReader. Can get current line number
    protected LineNumberReader reader;
    /* JSON Parser */
    protected ObjectMapper mapper;
    /* Reader Property */
    protected String uriName;
    protected long totalBytes = Long.MAX_VALUE;
    protected long bytesRead;
    protected boolean generateId = true;
    protected IdGenerator idGen;
    protected boolean hasNext = true;
    
    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
        if (instream != null) {
            instream.close();
        }
        
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return bytesRead/(float)totalBytes;
    }

    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {
        /* Initialization in super class */
        initConfig(context);  
        /*  Get file(s) in input split */
        setFile(((FileSplit) inSplit).getPath());
        // Initialize reader properties
        generateId = conf.getBoolean(CONF_INPUT_GENERATE_URI,false);
        if (generateId){
            idGen = new IdGenerator(file.toUri().getPath() + "-"
                    + ((FileSplit) inSplit).getStart()); 
        } else {
            uriName = conf.get(CONF_INPUT_URI_ID, null);
            mapper = new ObjectMapper();
        }
        bytesRead = 0;
        totalBytes = inSplit.getLength();
        /* Check file status */
        fs = file.getFileSystem(context.getConfiguration());
        FileStatus status = fs.getFileStatus(file);
        if (status.isDirectory()) {
            iterator = new FileIterator((FileSplit)inSplit, context);
            inSplit = iterator.next();
        }
        /* Initialize buffered reader */
        initFileStream(inSplit);
    }

    protected boolean findNextFileEntryAndInitReader() throws InterruptedException, IOException {
        if (iterator != null && iterator.hasNext()) {
            close();
            initFileStream(iterator.next());
            return true;
        } else {
            hasNext = false;
            return false;
        }

    }
    
    protected void initFileStream(InputSplit inSplit) 
            throws IOException, InterruptedException {
        fileIn = openFile(inSplit, true);
        if (fileIn == null) {
            return;
        }
        instream = new InputStreamReader(fileIn, encoding);
        reader = new LineNumberReader(instream);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (reader == null) {
            hasNext = false;
            return false;
        }
        String line = reader.readLine();
        int lineNumber = reader.getLineNumber();
        if (line == null) {
            if (findNextFileEntryAndInitReader()) {
                return nextKeyValue();
            } else { // End of the directory
                bytesRead = totalBytes;
                return false;
            }
        } else if (line.trim().equals("")) { 
            setSkipKey(lineNumber, 0, "empty lines");
            return true;
        } else if (line.startsWith(" ") || line.startsWith("\t")) {
            setSkipKey(lineNumber, 0, "leading space");
            return true;
        } else {
            if (generateId) {
                if (setKey(idGen.incrementAndGet(), lineNumber, 0, true)) {
                    return true;
                }
            } else {
                String uri = null;
                try {
                    uri = findUriInJSON(line.trim());
                    if (uri == null) {
                        setSkipKey(lineNumber, 0, 
                                "no qualifying URI value found");
                    } else {
                        setKey(uri, lineNumber, 0, true);
                    }
                } catch (Exception ex) {
                    setSkipKey(lineNumber, 0, ex.getMessage());
                }
                if (uri == null) {
                    return true;
                }
            }    
            if (value instanceof Text) {
                ((Text)value).set(line);
            } else {
                ((Text)((ContentWithFileNameWritable<VALUEIN>)
                        value).getValue()).set(line);
            } 
        }
        bytesRead += (long)line.getBytes().length;
        return true;  
    }
    
    @SuppressWarnings("unchecked")
    protected String findUriInJSON(String line) 
    throws JsonParseException, IOException {
        /* Breadth-First-Search */
        Queue<Object> q = new LinkedList<>();
        Object root = mapper.readValue(line.getBytes(), Object.class);
        if (root instanceof Map || root instanceof ArrayList) {
            q.add(root);
        } else {
            throw new UnsupportedOperationException("invalid JSON");
        }    
        while (!q.isEmpty()) {
            Object current = q.remove();
            if (current instanceof ArrayList) {
                for (Object element : (ArrayList<Object>)current) {
                    if (element instanceof Map || 
                        element instanceof ArrayList) {
                        q.add(element);
                    }
                }
            } else { // instanceof Map
                // First Match
                Map<String,?> map = (Map<String,?>)current;
                if (map.containsKey(uriName)) {
                    Object uriValue = map.get(uriName);
                    if (uriValue instanceof Number || 
                        uriValue instanceof String) {
                        return uriValue.toString();
                    } else {
                        return null;
                    }
                }
                // Add child elements to queue
                Iterator<?> it = map.entrySet().iterator();
                while (it.hasNext()) {
                    Entry<String,?> KVpair = (Entry<String,?>)it.next();
                    Object pairValue = KVpair.getValue();
                    
                    if (pairValue instanceof Map || 
                        pairValue instanceof ArrayList) {
                        q.add(pairValue);
                    }
                }
            }
        }
        return null;
    }  
}
