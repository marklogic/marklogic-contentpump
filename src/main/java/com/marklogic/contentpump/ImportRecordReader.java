/*
 * Copyright (c) 2024 MarkLogic Corporation
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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;

import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.DocumentURIWithSourceInfo;
import com.marklogic.mapreduce.MarkLogicConstants;
import com.marklogic.mapreduce.utilities.URIUtil;

/**
 * Abstract class of RecorderReader for import.
 * @author ali
 *
 * @param <VALUEIN>
 */
public abstract class ImportRecordReader<VALUEIN> 
extends RecordReader<DocumentURIWithSourceInfo, VALUEIN> 
implements ConfigConstants {
    public static final Log LOG = LogFactory.getLog(ImportRecordReader.class);
    protected DocumentURIWithSourceInfo key = new DocumentURIWithSourceInfo();
    protected VALUEIN value;
    protected String mode;
    protected boolean streaming = false;
    protected Configuration conf;
    protected String encoding;
    protected Path file;
    protected FileSystem fs;
    protected Iterator<FileSplit> iterator;
    private String srcId = null;
    protected String subId = "";
    
    /**
     * Apply URI replace option, encode URI if specified, apply URI prefix and
     * suffix configuration options and set the result as DocumentURI key.
     * 
     * @param uri Source string of document URI.
     * @param line Line number in the source if applicable; 0 otherwise.
     * @param col Column number in the source if applicable; 0 otherwise.
     * @param encode Encode uri if set to true.
     * 
     * @return true if key indicates the record is to be skipped; false 
     * otherwise.
     */
    protected boolean setKey(String uri, int line, int col, boolean encode) {
        if (key == null) {
            key = new DocumentURIWithSourceInfo(uri, srcId);
        }
        // apply prefix, suffix and replace for URI
        if (uri != null && !uri.isEmpty()) {
            uri = URIUtil.applyUriReplace(uri, conf);
            key.setSkipReason("");
            if (encode) {
                try {
                    URI uriObj = new URI(null, null, null, 0, uri, null, null);
                    uri = uriObj.toString();
                } catch (URISyntaxException e) {
                    uri = null;
                    key.setSkipReason(e.getMessage());
                }
            }
            uri = URIUtil.applyPrefixSuffix(uri, conf);
        } else {
            key.setSkipReason("empty uri value");
        }
        key.setUri(uri);   
        key.setSrcId(srcId);
        key.setSubId(subId);
        key.setColNumber(col);
        key.setLineNumber(line);     
    
        if (LOG.isTraceEnabled()) {
            LOG.trace("Set key: " + key);
        }     
        return key.isSkip();
    }

    /**
     * Set the result as DocumentURI key.
     *
     * @param line Line number in the source if applicable; -1 otherwise.
     * @param col Column number in the source if applicable; -1 otherwise.
     * @param reason
     * 
     * otherwise.
     */
    protected void setSkipKey(int line, int col, String reason) {
        if (key == null) {
            key = new DocumentURIWithSourceInfo("", srcId, subId, line, col);
        } else {
            key.setUri("");   
            key.setSrcId(srcId);
            key.setSubId(subId);
            key.setColNumber(col);
            key.setLineNumber(line);
        }
        key.setSkipReason(reason);

        if (LOG.isTraceEnabled()) {
            LOG.trace("Set key: " + key);
        }
    }

    @Override
    public abstract void close() throws IOException;

    @Override
    public DocumentURIWithSourceInfo getCurrentKey() throws IOException,
        InterruptedException {
        return key;
    }

    @Override
    public VALUEIN getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public abstract float getProgress() throws IOException,
        InterruptedException;

    @Override
    public abstract void initialize(InputSplit arg0, 
            TaskAttemptContext context) 
    throws IOException, InterruptedException;
    
    @SuppressWarnings("unchecked")
    protected void initConfig(TaskAttemptContext context) {
        conf = context.getConfiguration();
        String type = conf.get(MarkLogicConstants.CONTENT_TYPE,
            MarkLogicConstants.DEFAULT_CONTENT_TYPE);
        if (!conf.getBoolean(MarkLogicConstants.OUTPUT_STREAMING, false)) {
            ContentType contentType = ContentType.valueOf(type);
            Class<? extends Writable> valueClass = 
                contentType.getWritableClass();
            value = (VALUEIN) ReflectionUtils.newInstance(valueClass, conf);
        }
        encoding = conf.get(MarkLogicConstants.OUTPUT_CONTENT_ENCODING,
                DEFAULT_ENCODING);
    }

    @SuppressWarnings("unchecked")
    protected void configFileNameAsCollection(Configuration conf, Path file) {
        if (file == null) {
            return;
        }
        if (conf.getBoolean(CONF_OUTPUT_FILENAME_AS_COLLECTION, false)) {
            if (value instanceof ContentWithFileNameWritable) {
                ((ContentWithFileNameWritable<VALUEIN>) value)
                    .setFileName(file.getName());
            } else {
                Writable cvalue = new ContentWithFileNameWritable<>(
                    (VALUEIN) value, file.getName());
                value = (VALUEIN) cvalue;
            }
        }
    }

    protected String makeURIFromPath(Path file) {
        // get path portion of the file
       return file.toUri().getPath().toString();
    }
    
    protected String makeURIForZipEntry(Path zipFile, String val) {  
        Path path = new Path(zipFile, val);
        return path.toUri().getPath();
    }

    @Override
    public abstract boolean nextKeyValue() throws IOException,
        InterruptedException;

    public Path getFile() {
        return file;
    }

    public void setFile(Path file) {
        this.file = file;
        srcId = file.toString();
    }
    
    public FSDataInputStream openFile(InputSplit inSplit,
            boolean configCol) throws IOException {
        while (true) {
            setFile(((FileSplit) inSplit).getPath());
            if (configCol) {
                configFileNameAsCollection(conf, file);
            }
            try {
                return fs.open(file);
            } catch (IllegalArgumentException e){
                LOG.error("Input file skipped, reason: " + e.getMessage());
                if (iterator != null &&
                        iterator.hasNext()) {
                    inSplit = iterator.next();
                } else {
                    return null;
                }
            }
        }
    }
}
