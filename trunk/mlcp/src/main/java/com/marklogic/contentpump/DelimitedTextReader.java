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
import java.io.InputStreamReader;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVStrategy;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.marklogic.contentpump.utilities.FileIterator;
import com.marklogic.contentpump.utilities.IdGenerator;
import com.sun.org.apache.xml.internal.utils.XMLChar;

/**
 * Reader for DelimitedTextInputFormat.
 * @author ali
 *
 * @param <VALUEIN>
 */
public class DelimitedTextReader<VALUEIN> extends
    ImportRecordReader<VALUEIN> {
    public static final Log LOG = LogFactory.getLog(DelimitedTextReader.class);
    protected static final char encapsulator = '"';
    static final String DEFAULT_ROOT_NAME = "root";
    /**
     * header of delimited text
     */
    protected String[] fields;
    protected char delimiter;
    protected static String rootStart;
    protected static String rootEnd;
    protected CSVParser parser;
    protected InputStreamReader instream;
    protected boolean hasNext = true;
    protected String uriName; // the column name used for URI
    protected long fileLen = Long.MAX_VALUE;
    protected long bytesRead;
    protected boolean generateId;
    protected IdGenerator idGen;
    protected int uriId = -1; // the column index used for URI
    protected boolean compressed;
    
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
        initDelimConf();
        file = ((FileSplit) inSplit).getPath();
        fs = file.getFileSystem(context.getConfiguration());
        FileStatus status = fs.getFileStatus(file);
        if(status.isDir()) {
            iterator = new FileIterator((FileSplit)inSplit, context);
            inSplit = iterator.next();
        }
        initParser(inSplit);
    }
    
    protected void initParser(InputSplit inSplit) throws IOException, InterruptedException {
        file = ((FileSplit) inSplit).getPath();
        configFileNameAsCollection(conf, file);
        
        FSDataInputStream fileIn = fs.open(file);
        if (encoding == null) {
            instream = new InputStreamReader(fileIn);
        } else {
            instream = new InputStreamReader(fileIn, encoding);
            //String will be converted and read as UTF-8 String
        }
        bytesRead = 0;
        fileLen = inSplit.getLength();
        if (uriName == null) {
            generateId = conf.getBoolean(CONF_DELIMITED_GENERATE_URI, false);
            if (generateId) {
                idGen = new IdGenerator(file.toUri().getPath() + "-"
                        + ((FileSplit)inSplit).getStart());
            } else {
                uriId = 0;
            }
        }
        parser = new CSVParser(instream, new CSVStrategy(delimiter,
            encapsulator, CSVStrategy.COMMENTS_DISABLED,
            CSVStrategy.ESCAPE_DISABLED, true, true, false, true));
    }

    protected void initDelimConf() {
        String delimStr = conf.get(ConfigConstants.CONF_DELIMITER,
                ConfigConstants.DEFAULT_DELIMITER);
        if (delimStr.length() == 1) {
            delimiter = delimStr.charAt(0);
        } else {
            LOG.error("Incorrect delimitor: " + delimiter
                + ". Expects single character.");
        }
        uriName = conf.get(ConfigConstants.CONF_DELIMITED_URI_ID, null);
        String rootName = conf.get(CONF_DELIMITED_ROOT_NAME, 
                DEFAULT_ROOT_NAME);
        rootStart = '<' + rootName + '>';
        rootEnd = "</" + rootName + '>';

    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (parser == null) {
            return false;
        }
        try {
            String[] values = parser.getLine();

            if (values == null) {
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
            if (fields == null) {
                fields = values;
                boolean found = generateId || uriId == 0;
                for (int i = 0; i < fields.length && !found; i++) {
                    // Oracle jdk bug 4508058: UTF-8 encoding does not recognize
                    // initial BOM
                    // will not be fixed. Work Around :
                    // Application code must recognize and skip the BOM itself.
                    byte[] buf = fields[i].getBytes();
                    if (LOG.isDebugEnabled()) {
                        StringBuilder sb = new StringBuilder();
                        for (byte b : buf) {
                            sb.append(Byte.toString(b));
                            sb.append(" ");
                        }
                        LOG.debug(fields[i]);
                        LOG.debug(sb.toString());
                    }
                    if (buf[0] == (byte) 0xEF && buf[1] == (byte) 0xBB
                        && buf[2] == (byte) 0xBF) {
                        fields[i] = new String(buf, 3, buf.length - 3);
                    }

                    if (!XMLChar.isValidName(fields[i])) {
                        fields[i] = getValidName(fields[i]);
                    }
                    if (fields[i].equals(uriName)) {
                        uriId = i;
                        found = true;
                        break;
                    }
                }
                if (found == false) {
                    // idname doesn't match any columns
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Header: " + convertToLine(fields));
                    }
                    throw new IOException("Delimited_uri_id " + uriName
                        + " is not found.");
                }
                values = parser.getLine();

                if (values == null) {
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
            }

            if (values.length != fields.length) {
                LOG.error(file.toUri() + " line " + parser.getLineNumber()
                    + " is inconsistent with column definition: "
                    + convertToLine(values));
                key = null;
                return true;
            }
            StringBuilder sb = new StringBuilder();
            sb.append(rootStart);
            for (int i = 0; i < fields.length; i++) {
                if (!generateId && uriId == i) {
                    if (values[i] == null || values[i].equals("")) {
                        //TODO log file name and line number
                        LOG.error(convertToLine(fields)
                            + ":column used for uri_id is empty");
                        // clear the key of previous record
                        key = null;
                        return true;
                    }
                    String uri = getEncodedURI(values[i]);
                    if (uri != null) {
                        setKey(uri);
                    } else {
                        key = null;
                        return true;
                    }
                }
                sb.append('<').append(fields[i]).append('>');
                sb.append(convertToCDATA(values[i]));
                sb.append("</").append(fields[i]).append('>');
            }
            sb.append(rootEnd);
            if (generateId) {
                setKey(idGen.incrementAndGet());
            }
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
        } catch (IOException ex) {
            if (ex.getMessage().contains(
                "invalid char between encapsulated token end delimiter")) {
                LOG.error(ex.getMessage());
                key = null;
            } else {
                throw ex;
            }
        }
        return true;
    }
    
    private String convertToLine(String[] values) {
        StringBuilder sb = new StringBuilder();
        for (String s : fields) {
            sb.append(s);
            sb.append(delimiter);
        }
        return sb.substring(0, sb.length() - 1);
    }
    
    private String getValidName(String name) {
        StringBuilder validname = new StringBuilder();
        char ch = name.charAt(0);
        if(!XMLChar.isNameStart(ch)) {
            LOG.warn("Prepend _ to " + name);
            validname.append("_");
        }
        for (int i = 0; i < name.length(); i++ ) {
            ch = name.charAt(i);
            if (!XMLChar.isName(ch)) {
                LOG.warn("Character " + ch + " in " + name + " is converted to _");
                validname.append("_");
            } else {
                validname.append(ch);
            }
         }

        return validname.toString();
    }
    
    private String convertToCDATA(String arg) {
        StringBuilder sb = new StringBuilder();
        sb.append("<![CDATA[");
        sb.append(arg);
        sb.append("]]>");
        return sb.toString();
    }
}
