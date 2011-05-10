/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.marklogic.xcc.ResultItem;

/**
 * MarkLogicRecordReader for documents where the key is DocumentURI.
 * 
 * @author jchen
 */
public class DocumentReader extends MarkLogicRecordReader<DocumentURI, MarkLogicNode> {
	
	static final float DOCUMENT_TO_FRAGMENT_RATIO = 1; 
	
	/**
	 * Current key.
	 */
	private DocumentURI currentKey;
	/**
	 * Current value.
	 */
	private MarkLogicNode currentValue;
	
	public DocumentReader(Configuration conf) {
		super(conf);
	}

	@Override
	public DocumentURI getCurrentKey() throws IOException, InterruptedException {
		return currentKey;
	}
	
    protected void setCurrentKey(ResultItem item) {
    	if (item != null) {
		    currentKey = new DocumentURI(item.getDocumentURI());
    	} else {
    		currentKey = null;
    	}
	}

	@Override
    protected void endOfResult() {
	    currentKey = null;
	    currentValue = null;
    }

	@Override
    protected boolean nextResult(ResultItem result) {
		currentKey = new DocumentURI(result.getDocumentURI());
		currentValue = new MarkLogicNode(result);
	    return true;
    }

	@Override
    public MarkLogicNode getCurrentValue() throws IOException,
            InterruptedException {
	    return currentValue;
    }

	@Override
    protected float getDefaultRatio() {
	    return DOCUMENT_TO_FRAGMENT_RATIO;
    }
}
