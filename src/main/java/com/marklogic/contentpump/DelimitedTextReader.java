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
import java.nio.charset.Charset;
import java.util.Iterator;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.marklogic.contentpump.utilities.DocBuilder;
import com.marklogic.contentpump.utilities.CSVParserFormatter;
import com.marklogic.contentpump.utilities.EncodingUtil;
import com.marklogic.contentpump.utilities.FileIterator;
import com.marklogic.contentpump.utilities.IdGenerator;
import com.marklogic.contentpump.utilities.JSONDocBuilder;
import com.marklogic.contentpump.utilities.XMLDocBuilder;
import com.marklogic.mapreduce.MarkLogicConstants;

/**
 * Reader for DelimitedTextInputFormat.
 * 
 * @author ali
 *
 * @param <VALUEIN>
 */
public class DelimitedTextReader<VALUEIN> extends
    ImportRecordReader<VALUEIN> {
    public static final Log LOG = LogFactory.getLog(DelimitedTextReader.class);
    public static final char encapsulator = '"';
    /**
     * header of delimited text
     */
    protected String[] fields;
    protected char delimiter;
    protected CSVParser parser;
    protected InputStreamReader instream;
    protected FSDataInputStream fileIn;
    protected boolean hasNext = true;
    protected String uriName; // the column name used for URI
    protected long fileLen = Long.MAX_VALUE;
    protected long bytesRead;
    protected boolean generateId;
    protected IdGenerator idGen;
    protected int uriId = -1; // the column index used for URI
    protected boolean compressed;
    protected DocBuilder docBuilder;
    protected Iterator<CSVRecord> parserIterator;
    private int prevLineNumber = 1;
    
    @Override
    public void close() throws IOException {
        if (instream != null) {
            instream.close();
        }
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return bytesRead/fileLen;
    }

    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext context)
        throws IOException, InterruptedException {
        initConfig(context);
        initDocType();
        initDelimConf();
        setFile(((FileSplit) inSplit).getPath());
        fs = file.getFileSystem(context.getConfiguration());
        FileStatus status = fs.getFileStatus(file);
        if(status.isDirectory()) {
            iterator = new FileIterator((FileSplit)inSplit, context);
            inSplit = iterator.next();
        }
        initParser(inSplit);
    }
    
    protected void initParser(InputSplit inSplit) throws IOException,
        InterruptedException {
        fileIn = openFile(inSplit, true);
        if (fileIn == null) {
            return;
        }
        instream = new InputStreamReader(fileIn, encoding);

        bytesRead = 0;
        fileLen = inSplit.getLength();
        if (uriName == null) {
            generateId = conf.getBoolean(CONF_INPUT_GENERATE_URI, false);
            if (generateId) {
                idGen = new IdGenerator(file.toUri().getPath() + "-"
                    + ((FileSplit) inSplit).getStart());
            } else {
                uriId = 0;
            }
        }
        parser = new CSVParser(instream, CSVParserFormatter.
        		getFormat(delimiter, encapsulator, true,
        				true));
        parserIterator = parser.iterator();
    }

    protected void initDelimConf() {
        String delimStr = conf.get(ConfigConstants.CONF_DELIMITER,
                ConfigConstants.DEFAULT_DELIMITER);
        if (delimStr.length() == 1) {
            delimiter = delimStr.charAt(0);
        } else {  
            throw new UnsupportedOperationException("Invalid delimiter: " +
                    delimStr);
        }
        uriName = conf.get(ConfigConstants.CONF_INPUT_URI_ID, null);
        docBuilder.init(conf);
    }

    protected String[] getLine() throws IOException{
    	return getLine(getRecordLine());
    }

    protected String[] getLine(CSVRecord record)
            throws IOException {
        Iterator<String> recordIterator = record.iterator();
        int recordSize = record.size();
        String[] values = new String[recordSize];
        for (int i = 0; i < recordSize; i++) {
            if (recordIterator.hasNext()) {
                values[i] = (String)recordIterator.next();
            } else {
                throw new IOException("Record size doesn't match the real size");
            }
        }
        return values;
    }

    protected CSVRecord getRecordLine() {
        return (CSVRecord)parserIterator.next();
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (parser == null || parserIterator == null) {
            return false;
        }
        try {
            if (!parserIterator.hasNext()) {
                if(compressed) {
                    bytesRead = fileLen;
                    return false;
                } else { 
                    if (iterator != null && iterator.hasNext()) {
                        close();
                        initParser(iterator.next());
                        return nextKeyValue();
                    } else {
                        bytesRead = fileLen;
                        return false;
                    }
                }
            }
            String[] values = getLine();
            
            if (fields == null) {
                fields = values;
                if (Charset.defaultCharset().equals(Charset.forName("UTF-8"))) {
                    EncodingUtil.handleBOMUTF8(fields, 0);
                }
                boolean found = generateId || uriId == 0;
                for (int i = 0; i < fields.length && !found; i++) {
                    // delimiter
                    if (fields[i].equals(uriName)) {
                        uriId = i;
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    // idname doesn't match any columns
                    LOG.error("Skipped file: " + file.toUri()
                            + ", reason: " + URI_ID + " " + uriName
                            + " is not found");
                    parser = null;
                    return false;
                }
                try {
                    docBuilder.configFields(conf, fields);
                } catch (IllegalArgumentException e) {
                    LOG.error("Skipped file: " + file.toUri()
                            + ", reason: " + e.getMessage());
                    parser = null;
                    return false;
                }
                
                if (!parserIterator.hasNext()) {
                    if(compressed) {
                        bytesRead = fileLen;
                        return false;
                    } else { 
                        if (iterator != null && iterator.hasNext()) {
                            close();
                            initParser(iterator.next());
                            return nextKeyValue();
                        } else {
                            bytesRead = fileLen;
                            return false;
                        }
                    }
                }
                values = getLine();
            }
            int line = (int)parser.getCurrentLineNumber();
            // If the CSVParser is giving the same line number as previous
            // because there is no newline char at EOF, manually increase it.
            if (line == prevLineNumber) line++;
            prevLineNumber = line;
            if (values.length != fields.length) {
                setSkipKey(line, 0, 
                        "number of fields does not match number of columns");
                return true;
            }  
            docBuilder.newDoc();           
            for (int i = 0; i < fields.length; i++) {
                //skip the empty column in header
                if(fields[i].equals("")) {
                    continue;
                }
                if (!generateId && uriId == i) {
                    if (setKey(values[i], line, 0, true)) {
                        return true;
                    }
                }
                try {
                    docBuilder.put(fields[i], values[i]);
                } catch (Exception e) {
                    setSkipKey(line, 0, e.getMessage());
                    return true;
                }
            }
            docBuilder.build();
            if (generateId &&
                setKey(idGen.incrementAndGet(), line, 0, true)) {
                return true;
            }
            if (value instanceof Text) {
                ((Text)value).set(docBuilder.getDoc());
            } else {
                ((Text)((ContentWithFileNameWritable<VALUEIN>)
                        value).getValue()).set(docBuilder.getDoc());
            }
        } catch (RuntimeException ex) {
            if (ex.getMessage().contains(
                "invalid char between encapsulated token and delimiter")) {
                setSkipKey((int)parser.getCurrentLineNumber(), 0, 
                        "invalid char between encapsulated token and delimiter");
                // hasNext() will always be true here since this exception is caught
                if (parserIterator.hasNext()) {
                	// consume the rest fields of this line
                	parserIterator.next();
                }
            } else {
                throw ex;
            }
        }
        return true;
    }
    
    protected String convertToLine(String[] values) {
        StringBuilder sb = new StringBuilder();
        for (String s : values) {
            sb.append(s);
            sb.append(delimiter);
        }
        return sb.substring(0, sb.length() - 1);
    }
 
    protected void initDocType() {
        // CONTENT_TYPE validation is in Command.java: applyConfigOptions. 
        // We can assume here that the value in conf is always valid.
        String docType = conf.get(MarkLogicConstants.CONTENT_TYPE,
            MarkLogicConstants.DEFAULT_CONTENT_TYPE);
        
        if (docType.equals("XML")) {
            docBuilder = new XMLDocBuilder();
        } else {
            docBuilder = new JSONDocBuilder();
        }
    }

}
