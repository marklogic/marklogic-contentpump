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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.MarkLogicConstants;

public abstract class AbstractRecordReader<VALUEIN> extends
    RecordReader<DocumentURI, VALUEIN> {
    public static final Log LOG = LogFactory
        .getLog(AbstractRecordReader.class);
    protected DocumentURI key = new DocumentURI();
    protected VALUEIN value;
    protected String prefix;
    protected String suffix;

    protected void setKey(String uri) {
        StringBuilder sb = new StringBuilder();
        if (prefix != null) {
            sb.append(prefix);
        }
        sb.append(uri);
        if (suffix != null) {
            sb.append(suffix);
        }
        this.key.setUri(sb.toString());
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
    public abstract void initialize(InputSplit arg0, TaskAttemptContext context)
        throws IOException, InterruptedException;

    @SuppressWarnings("unchecked")
    protected void configFileNameAsCollection(Configuration conf, Path file) {
        if (file == null) {
            return;
        }
        String isFileAsCollection = conf.get(
            ConfigConstants.CONF_OUTPUT_FILENAME_AS_COLLECTION,
            ConfigConstants.DEFAULT_OUTPUT_FILENAME_AS_COLLECTION);
        if (isFileAsCollection.equalsIgnoreCase("true")) {
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
    
    @SuppressWarnings("unchecked")
    public void initCommonConfigurations(Configuration conf, Path file) {
        prefix = conf.get(ConfigConstants.CONF_OUTPUT_URI_PREFIX);
        suffix = conf.get(ConfigConstants.CONF_OUTPUT_URI_SUFFIX);
        String type = conf.get(MarkLogicConstants.CONTENT_TYPE,
            MarkLogicConstants.DEFAULT_CONTENT_TYPE);
        
        ContentType contentType = ContentType.valueOf(type);
        Class<? extends Writable> valueClass = contentType.getWritableClass();
        value = (VALUEIN) ReflectionUtils.newInstance(valueClass, conf);
        configFileNameAsCollection(conf, file);
        
    }

    @Override
    public abstract boolean nextKeyValue() throws IOException,
        InterruptedException;

}
