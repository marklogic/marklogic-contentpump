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

import com.marklogic.contentpump.ConfigConstants;
import com.sun.org.apache.xml.internal.utils.XMLChar;

/**
 * Builder for xml document.
 * @author mattsun
 *
 */
public class XMLDocBuilder extends DocBuilder {
    protected static String rootStart;
    protected static String rootEnd;
    static final String DEFAULT_ROOT_NAME = "root";
    
    /* 
     * @see com.marklogic.contentpump.DocBuilder#init(java.lang.String)
     */
    @Override
    public void init(Configuration conf) {
        String rootName = conf.get(ConfigConstants.CONF_DELIMITED_ROOT_NAME, 
                DEFAULT_ROOT_NAME);
        rootStart = '<' + rootName + '>';
        rootEnd = "</" + rootName + '>';
        
    }

    /* 
     * @see com.marklogic.contentpump.DocBuilder#newDoc()
     */
    @Override
    public void newDoc() throws IOException{
        sb = new StringBuilder();
        sb.append(rootStart);
        
    }

    /* 
     * @see com.marklogic.contentpump.DocBuilder#put(java.lang.String, java.lang.String)
     */
    @Override
    public void put(String key, String value) throws IOException {
        sb.append('<').append(key).append('>');
        sb.append(XMLUtil.convertToCDATA(value));
        sb.append("</").append(key).append('>');
        
    }

    /* 
     * @see com.marklogic.contentpump.DocBuilder#build()
     * @throws IOException
     */
    @Override
    public void build() throws IOException{
        sb.append(rootEnd);
        
    }

    /*
     * @see com.marklogic.contentpump.DocBuilder#checkDocumentHeader()
     * @throws IOException
     */
    @Override
    public void checkDocumentHeader(String[] fields) throws IOException {
        DocBuilder.checkXMLDocumentHeader(fields);
    }
    
}
