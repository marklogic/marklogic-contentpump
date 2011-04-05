/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Iterator;

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
extends MarkLogicRecordWriter<NodePath, MarkLogicNode> 
implements MarkLogicConstants {
	public static final Log LOG =
	    LogFactory.getLog(NodeWriter.class);
    
	private String queryTemp;
	private String namespace;
	
	public NodeWriter(URI serverUri, Collection<String> nsCol, String nodeOpType) {
		super(serverUri);
		queryTemp = NodeOpType.valueOf(nodeOpType).getQueryTemplate();
		StringBuilder buf = new StringBuilder();
		if (nsCol != null) {
			for (Iterator<String> nsIt = nsCol.iterator(); nsIt.hasNext();) {
				String ns = nsIt.next();
				buf.append('"').append(ns).append('"');
				if (nsIt.hasNext()) {
					buf.append(',');
				}
			}
		}
		this.namespace = buf.toString();
	}

	@Override
	public void write(NodePath path, MarkLogicNode record) 
	throws IOException, InterruptedException {
		String query = 
			queryTemp.replace(NODE_PATH_TEMPLATE, path.getFullPath())
			         .replace(NAMESPACE_TEMPLATE, namespace);
		String recordString = record == null ? "()" :
			record.toString();
		query = query.replace(NODE_STRING_TEMPLATE, recordString);
		if (LOG.isDebugEnabled()) {
		    LOG.info(query);
		}
		Session session = getSession();
		try {
			AdhocQuery request = session.newAdhocQuery(query);
			session.submitRequest(request);
		} catch (RequestException e) {	
			LOG.error(e);
			LOG.error(query);
			//throw new IOException(e);
		}
	}

}
