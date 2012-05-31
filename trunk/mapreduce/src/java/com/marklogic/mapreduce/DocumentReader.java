/*
 * Copyright (c) 2003-2012 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import com.marklogic.xcc.ResultItem;

/**
 * MarkLogicRecordReader for documents where the key is DocumentURI.
 * 
 * @author jchen
 */
public class DocumentReader<VALUEIN> 
extends MarkLogicRecordReader<DocumentURI, VALUEIN> {
    
    static final float DOCUMENT_TO_FRAGMENT_RATIO = 1; 
    
    /**
     * Current key.
     */
    private DocumentURI currentKey;
    /**
     * Current value.
     */
    private VALUEIN currentValue;
    private Class<? extends Writable> valueClass;
    
    public DocumentReader(Configuration conf) {
        super(conf);
        valueClass = conf.getClass(INPUT_VALUE_CLASS, MarkLogicDocument.class, 
                Writable.class);
    }

    @Override
    public DocumentURI getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
    }
    
    @Override
    protected void endOfResult() {
        currentKey = null;
        currentValue = null;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected boolean nextResult(ResultItem result) {
        if (currentKey != null) {
            currentKey.setUri(result.getDocumentURI());
        } else {
            currentKey = new DocumentURI(result.getDocumentURI());
        }
        if (currentValue == null) {
            currentValue = (VALUEIN)ReflectionUtils.newInstance(valueClass, 
                    getConf());
        }
        InternalUtilities.assignResultValue(valueClass, result, currentValue);
        return true;
    }

    @Override
    public VALUEIN getCurrentValue() throws IOException,
            InterruptedException {
        return currentValue;
    }

    @Override
    protected float getDefaultRatio() {
        return DOCUMENT_TO_FRAGMENT_RATIO;
    }
}
