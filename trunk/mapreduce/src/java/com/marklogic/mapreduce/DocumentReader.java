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
package com.marklogic.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import com.marklogic.mapreduce.utilities.InternalUtilities;
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
        valueClass = conf.getClass(INPUT_VALUE_CLASS, QueriedDocument.class, 
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
