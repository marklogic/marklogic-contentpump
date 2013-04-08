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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.marklogic.mapreduce.utilities.InternalUtilities;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.types.ValueType;

/**
 * MarkLogicRecordWriter that sets a property for a document.
 * 
 * @author jchen
 */
public class PropertyWriter 
extends MarkLogicRecordWriter<DocumentURI, MarkLogicNode> 
implements MarkLogicConstants {

    public static final Log LOG =
        LogFactory.getLog(PropertyWriter.class);
    public static final String DOCURI_VARIABLE_NAME = "uri";
    public static final String NODE_VARIABLE_NAME = "node";
    
    private String query;
    
    public PropertyWriter(Configuration conf, String host) {
        super(conf, host);
        String propOpType = conf.get(PROPERTY_OPERATION_TYPE, 
                DEFAULT_PROPERTY_OPERATION_TYPE);
        PropertyOpType opType = PropertyOpType.valueOf(propOpType);
        query = opType.getQuery(conf);
        if (LOG.isDebugEnabled()) {
            LOG.debug(query);
        }
    }

    @Override
    public void write(DocumentURI uri, MarkLogicNode record)
            throws IOException, InterruptedException {
        // initialize recordString
        String recordString = record == null ? "()" : record.toString();

        // execute query
        Session session = getSession();
        try {
            AdhocQuery request = session.newAdhocQuery(query);
            request.setNewStringVariable(DOCURI_VARIABLE_NAME, 
                    InternalUtilities.unparse(uri.getUri()));
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
