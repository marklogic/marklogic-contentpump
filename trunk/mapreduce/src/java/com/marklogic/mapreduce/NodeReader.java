package com.marklogic.mapreduce;

import java.io.IOException;

import com.marklogic.xcc.ResultItem;

/**
 * MarkLogicRecordReader for nodes where the key is NodePath.
 * @author jchen
 */
public class NodeReader extends MarkLogicRecordReader<NodePath> {

	/**
	 * Current key.
	 */
	private NodePath currentKey;
	
	public NodeReader(String serverUri, String pathExpr, String nameSpace) {
		super(serverUri, pathExpr, nameSpace);
	}

	@Override
	protected void setCurrentKey(ResultItem item) {
		if (item != null) {
			String uri = item.getDocumentURI();
			String path = item.getNodePath();
		    currentKey = new NodePath(uri, path);
    	} else {
    		currentKey = null;
    	}
	}

	@Override
	public NodePath getCurrentKey() throws IOException, InterruptedException {
		return currentKey;
	}

}
