package com.marklogic.mapreduce;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;

/**
 * MarkLogicRecordWriter to insert/replace a node to MarkLogic Server.
 * 
 * @author jchen
 */
public class NodeWriter 
extends MarkLogicRecordWriter<NodePath> 
implements MarkLogicConstants {
	public static final Log LOG =
	    LogFactory.getLog(NodeWriter.class);
    static final String NODE_PATH_TEMPLATE = "{node_path}";
    static final String NODE_STRING_TEMPLATE = "{node_string}";
    
	private String queryTemp;
	private String namespace;
	
	public NodeWriter(URI serverUri, String namespace, String nodeOpType) {
		super(serverUri);
		queryTemp = NodeOpType.valueOf(nodeOpType).getQueryTemplate();
		this.namespace = namespace;
	}

	@Override
	public void write(NodePath path, MarkLogicRecord record) 
	throws IOException, InterruptedException {
		String query = 
			queryTemp.replace(NODE_PATH_TEMPLATE, path.getFullPath())
			         .replace(NAMESPACE_TEMPLATE, namespace);
		String recordString = record == null ? "{}" :
			record.toString();
		query = query.replace(NODE_STRING_TEMPLATE, recordString);
		LOG.info(query);
		Session session = getSession();
		try {
			AdhocQuery request = session.newAdhocQuery(query);
			session.submitRequest(request);
		} catch (RequestException e) {	
			LOG.error(e);
			LOG.error(query);
			throw new IOException(e);
		}
	}

}
