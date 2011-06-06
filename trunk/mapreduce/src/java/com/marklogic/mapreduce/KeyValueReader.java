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
extends MarkLogicRecordReader<KEYIN, VALUEIN>
implements MarkLogicConstants {
    
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
    
    /**
     * Indicate whether a key has been fetched, and is waiting to fetch value
     * from the next result.
     */
    private boolean keyFetched;
    
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
        keyFetched = false;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected boolean nextResult(ResultItem result) {
        if (!keyFetched) {
            if (key == null) {
                key = (KEYIN)ReflectionUtils.newInstance(keyClass, 
                        getConf());
            }
            InternalUtilities.assignResultValue(keyClass, result, key);
        } else {
            if (value == null) {
                value = (VALUEIN)ReflectionUtils.newInstance(valueClass, 
                        getConf());
            }
            InternalUtilities.assignResultValue(valueClass, result, value);
        }
        keyFetched = !keyFetched;
        return true;
    }

    @Override
    protected float getDefaultRatio() {
        return VALUE_TO_FRAGMENT_RATIO;
    }
}
