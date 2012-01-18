/*
 * Copyright (c) 2003-2012 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;

import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.types.ValueType;
import com.marklogic.xcc.types.XName;

/**
 * MarkLogicRecordWriter for user specified key and value types.
 * 
 * @author jchen
 *
 * @param <KEYOUT>
 * @param <VALUEOUT>
 */
public class KeyValueWriter<KEYOUT, VALUEOUT> 
extends MarkLogicRecordWriter<KEYOUT, VALUEOUT> {
    
    private ValueType keyType;
    private ValueType valueType;
    private String statement;

    public KeyValueWriter(URI serverUri, Configuration conf) {
        super(serverUri, conf);
        
        String keyDataType = conf.get(OUTPUT_KEY_TYPE, "xs:string");
        keyType = ValueType.valueOf(keyDataType);
        String valueDataType = conf.get(OUTPUT_VALUE_TYPE, "xs:string");
        valueType = ValueType.valueOf(valueDataType);
        
        statement = conf.get(OUTPUT_QUERY);
    }

    @Override
    public void write(KEYOUT key, VALUEOUT value) throws IOException,
            InterruptedException {
        Session session = getSession();
        try {
            AdhocQuery request = session.newAdhocQuery(statement);
            request.setNewVariable(new XName(MR_NAMESPACE, OUTPUT_KEY_VARNAME),   
                    InternalUtilities.newValue(keyType, key));
            request.setNewVariable(new XName(MR_NAMESPACE, OUTPUT_VALUE_VARNAME), 
                    InternalUtilities.newValue(valueType, value));
         
            session.submitRequest(request);
            commitIfNecessary();
        } catch (RequestException e) {    
            LOG.error(e);
            LOG.error(statement);
            throw new IOException(e);
        }     
    }
}
