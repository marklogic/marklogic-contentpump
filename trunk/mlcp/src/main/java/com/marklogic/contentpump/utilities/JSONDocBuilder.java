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
package com.marklogic.contentpump.utilities;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
/**
 * Builder for json document.
 * @author mattsun
 *
 */
public class JSONDocBuilder extends DocBuilder {
    protected JsonFactory jsonFactory;
    protected JsonGenerator generator;
    protected ByteArrayOutputStream baos;
    /* 
     * @see com.marklogic.contentpump.DocBuilder#init(java.lang.String)
     */
    @Override
    public void init(Configuration conf) {
        jsonFactory = new JsonFactory();
        
    };

    /* 
     * @see com.marklogic.contentpump.DocBuilder#newDoc()
     */
    @Override
    public void newDoc() throws IOException {
        sb = new StringBuilder();
        baos = new ByteArrayOutputStream();
        generator = jsonFactory.createJsonGenerator(baos);
        generator.writeStartObject(); 
    }

    /* 
     * @see com.marklogic.contentpump.DocBuilder#put(java.lang.String, java.lang.String)
     */
    @Override
    public void put(String key, String value) throws IOException {
        generator.writeStringField(key, value); 
        
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
    public void checkDocumentHeader(String[] fields) throws IOException {}
    
}
