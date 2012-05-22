package com.marklogic.contentpump;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;

import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.MarkLogicConstants;

public class SequenceFileReader<VALUEIN> extends AbstractRecordReader<VALUEIN> {
    protected SequenceFile.Reader reader;
    protected Writable seqKey;
    protected Writable seqValue;
    protected boolean hasNext = true;

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

    @SuppressWarnings("unchecked")
    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext context)
        throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Path file = ((FileSplit) inSplit).getPath();

        prefix = conf.get(ConfigConstants.CONF_OUTPUT_URI_PREFIX);
        suffix = conf.get(ConfigConstants.CONF_OUTPUT_URI_SUFFIX);

        FileSystem fs = file.getFileSystem(context.getConfiguration());
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

        configFileNameAsCollection(conf, file);

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
                ((Text) value).set(((SequenceFileValue<Text>) seqValue)
                    .getValue());
            } else if (value instanceof BytesWritable) {
                ((BytesWritable) value)
                    .set(((SequenceFileValue<BytesWritable>) seqValue)
                        .getValue());
            } else if (value instanceof ContentWithFileNameWritable) {
                VALUEIN realValue = ((ContentWithFileNameWritable<VALUEIN>) value)
                    .getValue();
                if (realValue instanceof Text) {
                    ((Text) realValue)
                        .set(((SequenceFileValue<Text>) seqValue).getValue());
                } else if (realValue instanceof BytesWritable) {
                    ((BytesWritable) realValue)
                        .set(((SequenceFileValue<BytesWritable>) seqValue)
                            .getValue());
                }
            }
            return true;
        }
        return false;
    }

}
