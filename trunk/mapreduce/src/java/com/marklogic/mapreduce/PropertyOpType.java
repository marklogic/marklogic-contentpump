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

import org.apache.hadoop.conf.Configuration;

/**
 * Type of supported property operations.
 * 
 * <p>
 *  When using {@link PropertyOutputFormat}, set the configuration property
 *  <code>mapreduce.marklogic.output.property.optype</code> to
 *  one of these values to control how the output property value is 
 *  handled by the server.
 * </p>
 * <p>
 *  Use <code>SET_PROPERTY</code> to replace any existing properties with
 *  the new property. Use <code>ADD_PROPERTY</code> to add a property
 *  without removing existing properties.
 * </p>
 * <p>
 *  For more information, see the following built-in functions in the
 *  <em>XQuery & XSLT API Reference</em>:
 *  <ul>
 *   <li>xdmp:document-set-property</li>
 *   <li>xdmp:document-add-properties</li>
 *  </ul>
 * </p>
 * 
 * @author jchen
 */

public enum PropertyOpType {
    SET_PROPERTY {
        public String getFunctionName() {
            return "xdmp:document-set-property";
        }
    },
    ADD_PROPERTY {
        public String getFunctionName() {
            return "xdmp:document-add-properties";
        }
    };
    
    abstract public String getFunctionName();
    
    public String getQuery(Configuration conf) {
        boolean alwaysCreate = conf.getBoolean(
                MarkLogicConstants.OUTPUT_PROPERTY_ALWAYS_CREATE, false);
        
        StringBuilder buf = new StringBuilder();
        buf.append("xquery version \"1.0-ml\"; \n");
        buf.append("declare variable $");
        buf.append(PropertyWriter.DOCURI_VARIABLE_NAME);
        buf.append(" as xs:string external;\n");
        buf.append("declare variable $");
        buf.append(PropertyWriter.NODE_VARIABLE_NAME);
        buf.append(" as element() external;\n");
        if (!alwaysCreate) {
            buf.append("let $exist := fn:exists(fn:doc($");
            buf.append(PropertyWriter.DOCURI_VARIABLE_NAME);
            buf.append("))\nreturn if ($exist) then \n");
        }
        buf.append(getFunctionName());
        buf.append("($");
        buf.append(PropertyWriter.DOCURI_VARIABLE_NAME);
        buf.append(", $");
        buf.append(PropertyWriter.NODE_VARIABLE_NAME);
        if (!alwaysCreate) {
            buf.append(") else ()");
        } else {
            buf.append(")");
        }
        
        return buf.toString();
    }
}
