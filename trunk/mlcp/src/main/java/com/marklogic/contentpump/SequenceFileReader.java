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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;

import com.marklogic.contentpump.utilities.FileIterator;
import com.marklogic.mapreduce.MarkLogicConstants;

/**
 * Reader for SequenceFileInputFormat.
 * @author ali
 *
 * @param <VALUEIN>
 */
public class SequenceFileReader<VALUEIN> extends ImportRecordReader<VALUEIN> {
    public static final Log LOG = LogFactory.getLog(SequenceFileReader.class);
    protected SequenceFile.Reader reader;
    protected Writable seqKey;
    protected Writable seqValue;
    protected boolean hasNext = true;
    protected int batchSize;
    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return hasNext == true ? 0 : 1;
    }

    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext context)
        throws IOException, InterruptedException {
        initConfig(context);
        batchSize = conf.getInt(MarkLogicConstants.BATCH_SIZE, 
            MarkLogicConstants.DEFAULT_BATCH_SIZE);
        
        file = ((FileSplit) inSplit).getPath();
        fs = file.getFileSystem(context.getConfiguration());
        FileStatus status = fs.getFileStatus(file);
        if(status.isDir()) {
            iterator = new FileIterator((FileSplit)inSplit, context);
            inSplit = iterator.next();
        }
        
        initReader(inSplit);

    }

    @SuppressWarnings("unchecked")
    protected void initReader(InputSplit inSplit) throws IOException{
        file = ((FileSplit) inSplit).getPath();
        reader = new SequenceFile.Reader(fs, file, conf);
        String keyClass = conf
            .get(ConfigConstants.CONF_INPUT_SEQUENCEFILE_KEY_CLASS);
        String valueClass = conf
            .get(ConfigConstants.CONF_INPUT_SEQUENCEFILE_VALUE_CLASS);
        String valueType = conf
            .get(ConfigConstants.CONF_INPUT_SEQUENCEFILE_VALUE_TYPE);
        SequenceFileValueType svType = SequenceFileValueType
            .valueOf(valueType);
        Class<? extends Writable> vClass = svType.getWritableClass();
        value = (VALUEIN) ReflectionUtils.newInstance(vClass, conf);

        if (!reader.getKeyClass().getCanonicalName().equals(keyClass)) {
            throw new IOException("Key class of sequence file on HDFS is "
                + keyClass
                + "which is inconsistent with the one in configuration "
                + reader.getKeyClass().getCanonicalName());
        }
        if (!reader.getValueClass().getCanonicalName().equals(valueClass)) {
            throw new IOException("Value class of sequence file on HDFS is "
                + valueClass
                + "which is inconsistent with the one in configuration "
                + reader.getValueClass().getCanonicalName());
        }
        seqKey = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(),
            conf);
        seqValue = (Writable) ReflectionUtils.newInstance(
            reader.getValueClass(), conf);
    }
    @SuppressWarnings("unchecked")
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (reader == null) {
            return false;
        }

        while (reader.next(seqKey, seqValue)) {
            setKey(((SequenceFileKey) seqKey).getDocumentURI().getUri());

            if (value instanceof Text) {
                ((Text) value)
                    .set(new String(((SequenceFileValue<Text>) seqValue)
                        .getValue().getBytes(), "UTF-8"));
            } else if (value instanceof BytesWritable) {
                if (batchSize > 1) {
                    value = (VALUEIN) new BytesWritable(
                        ((SequenceFileValue<BytesWritable>) seqValue)
                            .getValue().getBytes());
                } else {
                    ((BytesWritable) value).set(new BytesWritable(
                        ((SequenceFileValue<BytesWritable>) seqValue)
                            .getValue().getBytes()));
                }
            } else {
                LOG.error("Unexpected type: " + value.getClass());
                key = null;
            }
            return true;
        }
        //end of seq file
        if (iterator != null && iterator.hasNext()) {
            close();
            initReader(iterator.next());
            return nextKeyValue();
        }
        return false;
    }
}
