package com.marklogic.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.marklogic.xcc.ResultItem;

/**
 * MarkLogicRecordReader for nodes where the key is NodePath.
 * @author jchen
 */
public class NodeReader extends MarkLogicRecordReader<NodePath, MarkLogicNode> {

	public NodeReader(Configuration conf, String serverUriTemp) {
	    super(conf, serverUriTemp);
    }

	/**
	 * Current key.
	 */
	private NodePath currentKey;
	/**
	 * Current value.
	 */
	private MarkLogicNode currentValue;

	@Override
	public NodePath getCurrentKey() throws IOException, InterruptedException {
		return currentKey;
	}

	@Override
    protected void endOfResult() {
	    currentKey = null;
	    currentValue = null;
    }

	@Override
    protected boolean nextResult(ResultItem result) {
		String uri = result.getDocumentURI();
		String path = result.getNodePath();
	    currentKey = new NodePath(uri, path);
		currentValue = new MarkLogicNode(result);
		return true;
    }

	@Override
    public MarkLogicNode getCurrentValue() throws IOException,
            InterruptedException {
	    return currentValue;
    }
}
