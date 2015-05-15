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

import org.apache.hadoop.conf.Configuration;

import com.marklogic.mapreduce.utilities.InternalUtilities;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.types.ValueType;
import com.marklogic.xcc.types.XName;
import com.marklogic.xcc.RequestOptions;

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
    private String queryLanguage;

    public KeyValueWriter(Configuration conf, String host) {
        super(conf, host);
        
        String keyDataType = conf.get(OUTPUT_KEY_TYPE, "xs:string");
        keyType = ValueType.valueOf(keyDataType);
        String valueDataType = conf.get(OUTPUT_VALUE_TYPE, "xs:string");
        valueType = ValueType.valueOf(valueDataType);
        
        statement = conf.get(OUTPUT_QUERY);
        queryLanguage = conf.get(OUTPUT_QUERY_LANGUAGE);
    }

    @Override
    public void write(KEYOUT key, VALUEOUT value) throws IOException,
            InterruptedException {
        Session session = getSession();
        try {
            AdhocQuery request = session.newAdhocQuery(statement);
            if (queryLanguage != null) {
            	RequestOptions options = new RequestOptions();
            	options.setQueryLanguage(queryLanguage);
                request.setOptions(options);
            }
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
