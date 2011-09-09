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
import org.apache.hadoop.conf.Configuration;

import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.types.ValueType;

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
    public static final String NODE_VARIABLE_NAME = "node";
    public static final String PATH_VARIABLE_NAME = "path";
    
    private NodeOpType opType;
    private String namespace;
    
    public NodeWriter(URI serverUri, Configuration conf) {
        super(serverUri, conf);
        opType = NodeOpType.valueOf(conf.get(NODE_OPERATION_TYPE));
        Collection<String> nsCol = conf.getStringCollection(OUTPUT_NAMESPACE);
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
        String recordString = record == null ? "()" :
            record.toString();
        String query = opType.getQuery(namespace);
 
        if (LOG.isDebugEnabled()) {
            LOG.debug(query);
        }
        Session session = getSession();
        try {
            AdhocQuery request = session.newAdhocQuery(query);
            request.setNewStringVariable(PATH_VARIABLE_NAME, path.getFullPath());
            request.setNewVariable(NODE_VARIABLE_NAME, ValueType.ELEMENT, 
                    recordString);
            session.submitRequest(request);
            commitIfNecessary();
        } catch (RequestException e) {    
            LOG.error(e);
            LOG.error(query);
            throw new IOException(e);
        }
    }

}
