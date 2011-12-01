/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;

import com.marklogic.xcc.ResultItem;

/**
 * <p>MarkLogicRecordReader that fetches data from MarkLogic server and generates 
 * key value pairs in user specified types.</p>
 * 
 * @author jchen
 *
 * @param <KEYIN>
 * @param <VALUEIN>
 */
public class KeyValueReader<KEYIN, VALUEIN>
extends MarkLogicRecordReader<KEYIN, VALUEIN> {
    
    public static final Log LOG =
        LogFactory.getLog(KeyValueReader.class);
    
    static final float VALUE_TO_FRAGMENT_RATIO = 100;
    
    /**
     * Current key.
     */
    private KEYIN key;

    /**
     * Current value.
     */
    private VALUEIN value;
    @SuppressWarnings("unchecked")
    private Class<? extends WritableComparable> keyClass;
    private Class<? extends Writable> valueClass;
    
    public KeyValueReader(Configuration conf) {
        super(conf);
        keyClass = conf.getClass(INPUT_KEY_CLASS, Text.class, 
                WritableComparable.class);
        valueClass = conf.getClass(INPUT_VALUE_CLASS, Text.class, 
                Writable.class);    
    }

    @Override
    public VALUEIN getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public KEYIN getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    protected void endOfResult() {
        key = null;
        value = null;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (result != null && result.hasNext()) {
            if (key == null) {
                key = (KEYIN)ReflectionUtils.newInstance(keyClass, 
                          getConf());
            }
            ResultItem item = result.next();
            InternalUtilities.assignResultValue(keyClass, item, key);
            if (result.hasNext()) {
                if (value == null) {
                    value = (VALUEIN)ReflectionUtils.newInstance(valueClass, 
                               getConf());
                }
                item = result.next();
                InternalUtilities.assignResultValue(valueClass, item, value);
                count++;
                return true;
            }
        }
        endOfResult();
        return false;
    }

    @Override
    protected boolean nextResult(ResultItem result) {
        return false;
    }

    @Override
    protected float getDefaultRatio() {
        return VALUE_TO_FRAGMENT_RATIO;
    }
}
