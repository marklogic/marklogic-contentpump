/*
 * Copyright 2003-2015 MarkLogic Corporation

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
import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.w3c.dom.Node;

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
    
    private String query;
    
    public NodeWriter(Configuration conf, String host) {
        super(conf, host);
        String opTypeStr = conf.get(NODE_OPERATION_TYPE);
        if (opTypeStr == null || opTypeStr.isEmpty()) {
            throw new IllegalArgumentException(
                    NODE_OPERATION_TYPE + " is not specified.");
        }
        NodeOpType opType = NodeOpType.valueOf(opTypeStr);
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
        query = opType.getQuery(buf.toString());
    }

    @Override
    public void write(NodePath path, MarkLogicNode record) 
    throws IOException, InterruptedException {
        if (record == null || record.get() == null) {
            throw new UnsupportedOperationException("Record node is null.");
        } else if (record.get().getNodeType() != Node.ELEMENT_NODE) {
            throw new UnsupportedOperationException("Unsupported node type: " +
                    record.get().getNodeType());
        }
        String recordString = record.toString();
 
        if (LOG.isDebugEnabled()) {
            LOG.debug(query);
        }
        Session session = getSession();
        try {
            AdhocQuery request = session.newAdhocQuery(query);
            request.setNewStringVariable(PATH_VARIABLE_NAME, path.getFullPath());
            
            request.setNewVariable(NODE_VARIABLE_NAME, ValueType.ELEMENT, 
                    recordString);
            if (LOG.isDebugEnabled()) {
                LOG.debug("path: " + path.getFullPath());
                LOG.debug("node: " + recordString);
            }
            session.submitRequest(request);
            commitIfNecessary();
        } catch (RequestException e) {    
            LOG.error(e);
            LOG.error(query);
            throw new IOException(e);
        }
    }

}
