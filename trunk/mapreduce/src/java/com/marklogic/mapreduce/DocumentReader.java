package com.marklogic.mapreduce;

import java.io.IOException;

import com.marklogic.xcc.ResultItem;

/**
 * MarkLogicRecordReader for documents where the key is DocumentURI.
 * 
 * @author jchen
 */
public class DocumentReader extends MarkLogicRecordReader<DocumentURI> {
	/**
	 * Current key.
	 */
	private DocumentURI currentKey;
	
	public DocumentReader(String serverUri, String pathExpr, String nameSpace) {
		super(serverUri, pathExpr, nameSpace);
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
}
