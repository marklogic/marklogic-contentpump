/*
 * Copyright (c) 2023 MarkLogic Corporation
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
package com.marklogic.contentpump.utilities;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import com.marklogic.contentpump.ColumnDataType;
import com.marklogic.contentpump.ConfigConstants;
/**
 * Builder for json document.
 * @author mattsun
 *
 */
public class JSONDocBuilder extends DocBuilder {
    public static final Log LOG = LogFactory.getLog(JSONDocBuilder.class);
    protected JsonFactory jsonFactory;
    protected JsonGenerator generator;
    protected ByteArrayOutputStream baos;
    protected Map<String,ColumnDataType> datatypeMap;
    /* 
     * @see com.marklogic.contentpump.DocBuilder#init(java.lang.String)
     */
    @Override
    public void init(Configuration conf) {
        jsonFactory = new JsonFactory();
        
    }
    
    /* 
     * @see com.marklogic.contentpump.DocBuilder#newDoc()
     */
    @Override
    public void newDoc() throws IOException {
        sb = new StringBuilder();
        baos = new ByteArrayOutputStream();
        generator = jsonFactory.createGenerator(baos);
        generator.writeStartObject(); 
    }

    /* 
     * @see com.marklogic.contentpump.DocBuilder#put(java.lang.String, java.lang.String)
     */
    @Override
    public void put(String key, String value) throws Exception{
        try {
            Object valueObj = datatypeMap.get(key).parse(value);
            generator.writeObjectField(key, valueObj);
        } catch (ParseException e) {
            throw new ParseException("Value " + value + " is not type "
                    + datatypeMap.get(key).name(), 0);
        } catch (Exception e) {
            String msg = e.getMessage();
            if (!msg.contains("missing value")) {
                throw new Exception(msg);
            }
        }       
        
    }

    /* 
     * @see com.marklogic.contentpump.DocBuilder#build()
     * @throws IOException
     */
    @Override
    public void build() throws IOException{
        generator.writeEndObject();
        generator.close();
        sb.append(baos.toString());
        
    }
    
    /*
     * @see com.marklogic.contentpump.DocBuilder#checkDocumentHeader()
     * @throws IOException
     */
    @Override
    public void configFields(Configuration conf, String[] fields) 
            throws IllegalArgumentException, IOException {
        if (null != fields) {
            super.configFields(conf, fields);
        } else {
            throw new IOException("Fields not defined");
        }
        
        datatypeMap = new HashMap<>();
        for (String s: fields) {
            datatypeMap.put(s, ColumnDataType.STRING);
        }
        String list = conf.get(ConfigConstants.CONF_DELIMITED_DATA_TYPE);
        if (list == null) {
            return;
        }
        String pairs[] = list.split(",");
        for (int i = 0; i < pairs.length; i += 2) {
            String colName = pairs[i].trim();
            String colDataType = pairs[i+1].trim();
            if (!datatypeMap.containsKey(colName)) {
                throw new IllegalArgumentException("Column name " + colName + " not found.");
            }
            if ("Number".equalsIgnoreCase(colDataType)) {
                datatypeMap.put(colName, ColumnDataType.NUMBER);
            } else if ("Boolean".equalsIgnoreCase(colDataType)) {
                datatypeMap.put(colName, ColumnDataType.BOOLEAN);
            } else if (!"String".equalsIgnoreCase(colDataType)) {
                throw new IllegalArgumentException("Unsupported column data type " + colDataType);
            }
        }
    }
    
}
