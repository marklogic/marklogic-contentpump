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
// TODO: Change to MarkLogicDocument when 12964 is fixed.
public class DocumentReader extends MarkLogicRecordReader<DocumentURI, MarkLogicNode> {
	/**
	 * Current key.
	 */
	private DocumentURI currentKey;
	/**
	 * Current value.
	 */
	private MarkLogicNode currentValue;
	
	public DocumentReader(Configuration conf, String serverUri) {
		super(conf, serverUri);
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
}
