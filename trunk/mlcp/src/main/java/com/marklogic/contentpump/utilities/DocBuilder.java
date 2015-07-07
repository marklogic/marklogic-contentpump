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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.sun.org.apache.xml.internal.utils.XMLChar;

/**
 * 
 * Abstract class for document builder.
 * @author mattsun
 *
 */
public abstract class DocBuilder {
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
    public abstract void newDoc () throws IOException;
    
    /**
     * Put a new key-value pair into the document.
     * @param key
     * @param value
     * @throws IOException
     */
    public abstract void put(String key, String value) throws IOException;
    
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
    public abstract void checkDocumentHeader(String[] fields) throws IOException;
    
    /**
     * Check valid chars for xml doc.
     * @param fields
     * @throws IOException
     */
    public static void checkXMLDocumentHeader(String[] fields) throws IOException {
        if (fields != null) {
            for (int i = 0; i < fields.length; i++) {
                if(fields[i].trim().equals("")) continue;
                if (!XMLChar.isValidName(fields[i])) {
                    fields[i] = XMLUtil.getValidName(fields[i]);
                }
            }
        } else {
            throw new IOException("Fields are not defined.");
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
