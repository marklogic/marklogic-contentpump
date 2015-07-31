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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
    protected String srcId = null;
    protected String subId = "";
    
    /**
     * Apply URI prefix and suffix configuration options and set the result as 
     * DocumentURI key.
     * 
     * @param uri Source string of document URI.
     * @param line Line number in the source if applicable; -1 otherwise.
     * @param col Column number in the source if applicable; -1 otherwise.
     */
    protected void setKey(String uri, int line, int col) {
        if (srcId == null) {
            srcId = file == null ? "" : file.toString();
        }
        if (key == null) {
            key = new DocumentURIWithSourceInfo(uri, srcId, subId, line, col);
        }
        // apply prefix and suffix for URI
        if (uri != null && !uri.isEmpty()) {
            uri = URIUtil.applyPrefixSuffix(uri, conf);
        }
        key.setUri(uri == null ? "" : uri);   
        key.setSrcId(srcId);
        key.setSubId(subId);
        key.setColNumber(col);
        key.setLineNumber(line);
        key.setSkipReason("");
        if (uri == null || uri.isEmpty()) {
            key.setSkip(true);
        } else {
            key.setSkip(false);
        }
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
                Writable cvalue = new ContentWithFileNameWritable<VALUEIN>(
                    (VALUEIN) value, file.getName());
                value = (VALUEIN) cvalue;
            }
        }
    }

    protected String makeURIFromPath(Path file) throws URISyntaxException {
        // get path portion of the file
       String path = file.toUri().getPath();
       if (LOG.isTraceEnabled()) {
         LOG.trace("makeURIFromPath Path:"+path);
       }
       // apply URI replace
       path = URIUtil.applyUriReplace(path, conf);
       if (LOG.isTraceEnabled()) {
         LOG.trace("makeURIFromPath Path after uri replace:"+path);
       }
       // create a URI out of it
       URI uri = new URI(null, null, null, 0, path, null, null);
       // return the encoded uri as document uri key
       if (LOG.isTraceEnabled()) {
           LOG.trace("makeURIFromPath document URI" + uri);
       }
       return uri.toString();
    }
    
    protected String getEncodedURI(String val) {  
        val = URIUtil.applyUriReplace(val, conf);
        try {
            URI uri = new URI(null, null, null, 0, val, null, null);
            return uri.toString();
        } catch (URISyntaxException e) {
            LOG.warn("Error parsing value as URI, skipping " + val, e);
            return null;
        }
    }
    
    protected String makeURIForZipEntry(Path zipFile, String val) 
    throws URISyntaxException {  
        Path path = new Path(zipFile, val);
        val = URIUtil.applyUriReplace(path.toUri().getPath(), conf);
        URI uri = new URI(null, null, null, 0, val, null, null);
        return uri.toString();
    }

    @Override
    public abstract boolean nextKeyValue() throws IOException,
        InterruptedException;
}
