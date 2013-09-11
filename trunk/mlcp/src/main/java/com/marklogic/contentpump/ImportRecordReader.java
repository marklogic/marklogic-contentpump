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

import com.marklogic.contentpump.utilities.URIUtil;
import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.MarkLogicConstants;

/**
 * Abstract class of RecorderReader for import.
 * @author ali
 *
 * @param <VALUEIN>
 */
public abstract class ImportRecordReader<VALUEIN> extends
    RecordReader<DocumentURI, VALUEIN> implements ConfigConstants {
    public static final Log LOG = LogFactory.getLog(ImportRecordReader.class);
    protected DocumentURI key = new DocumentURI();
    protected VALUEIN value;
    protected String mode;
    protected boolean streaming = false;
    protected Configuration conf;
    protected String encoding;
    protected Path file;
    protected FileSystem fs;
    protected Iterator<FileSplit> iterator;
    /**
     * Apply URI prefix and suffix configuration options and set the result as 
     * DocumentURI key.
     * 
     * @param uri Source string of document URI.
     */
    protected void setKey(String uri) {
        if (uri == null) {
            key = null;
            return;
        }
        // reconstruct key if set to null
        if (key == null) {
            key = new DocumentURI();
        }
        // apply prefix and suffix for URI
        uri = URIUtil.applyPrefixSuffix(uri, conf);
        key.setUri(uri);      
    }

    @Override
    public abstract void close() throws IOException;

    @Override
    public DocumentURI getCurrentKey() throws IOException,
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
        encoding = conf.get(MarkLogicConstants.OUTPUT_CONTENT_ENCODING);
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

    protected String makeURIFromPath(Path file) {
        // get path portion of the file
       String path = file.toUri().getPath();
       
       // apply URI replace
       path = URIUtil.applyUriReplace(path, conf);
       
       // create a URI out of it
       try {
           URI uri = new URI(null, null, null, 0, path, null, null);
           // return the encoded uri as document uri key
           return uri.toString();
       } catch (URISyntaxException ex) {
           LOG.warn("Error parsing file path, skipping " + file, ex);
           return null;
       }
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
    
    protected String makeURIForZipEntry(Path zipFile, String val) {  
        Path path = new Path(zipFile, val);
        val = URIUtil.applyUriReplace(path.toUri().getPath(), conf);
        try {
            URI uri = new URI(null, null, null, 0, val, null, null);
            return uri.toString();
        } catch (URISyntaxException e) {
            LOG.warn("Error parsing value as URI, skipping " + val, e);
            return null;
        }
    }

    @Override
    public abstract boolean nextKeyValue() throws IOException,
        InterruptedException;
}
