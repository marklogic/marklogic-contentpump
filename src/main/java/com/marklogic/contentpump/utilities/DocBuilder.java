/*
 * Copyright (c) 2021 MarkLogic Corporation
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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * 
 * Abstract class for document builder.
 * @author mattsun
 *
 */
public abstract class DocBuilder {
    public static final Log LOG = 
            LogFactory.getLog(DocBuilder.class);
    protected StringBuilder sb;
    
    /**
     * Init the document.
     * @param conf
     */
    public abstract void init(Configuration conf);
    
    /**
     * Start to build the document.
     * @throws IOException
     */
    public abstract void newDoc() throws IOException;
    
    /**
     * Put a new key-value pair into the document.
     * @param key
     * @param value
     * @throws IOException
     */
    public abstract void put(String key, String value) throws Exception;
    
    /**
     * Build the document.
     * @throws IOException 
     */
    public abstract void build() throws IOException;
    
    /**
     * Check document header fields.
     * @param fields
     * @throws IOException
     */
    public void configFields(Configuration conf, String[] fields) 
            throws IllegalArgumentException, IOException {
        for (int i = 0; i < fields.length; i++) {
            fields[i] = fields[i].trim();
            if ("".equals(fields[i])) {
                LOG.warn("Column " + (i+1) + " has no header and will be skipped");
            }
        }
    }

    /**
     * Return the built doc.
     * @return
     */
    public String getDoc() {
        return sb.toString();
    }
    
}
