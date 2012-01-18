/*
 * Copyright (c) 2003-2012 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import com.marklogic.xcc.ResultItem;

/**
 * <p>A RecordReader that fetches data from MarkLogic server and generates 
 * an integer key for each value fetched.</p>
 * 
 * @author jchen
 * 
 * @param <VALUEIN>
 */
public class ValueReader<VALUEIN> 
extends MarkLogicRecordReader<LongWritable, VALUEIN>
implements MarkLogicConstants {

    static final float VALUE_TO_FRAGMENT_RATIO = 100; 
    
    public static final Log LOG =
        LogFactory.getLog(ValueReader.class);
    private LongWritable key;
    private VALUEIN value;
    private Class<? extends Writable> valueClass;

    public ValueReader(Configuration conf) {
        super(conf);
        valueClass = conf.getClass(INPUT_VALUE_CLASS, Text.class, 
                Writable.class);
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public VALUEIN getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    protected void endOfResult() {
        key = null;
        value = null;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected boolean nextResult(ResultItem result) {
        if (key == null) {
            key = new LongWritable(getCount());
        } else {
            key.set(getCount());
        }
        if (value == null) {
            value = (VALUEIN)ReflectionUtils.newInstance(valueClass, 
                    getConf());
        }
        InternalUtilities.assignResultValue(valueClass, result, value);
        
        return true;
    }

    @Override
    protected float getDefaultRatio() {
        return VALUE_TO_FRAGMENT_RATIO;
    }

}
