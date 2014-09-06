/*
 * Copyright 2003-2014 MarkLogic Corporation
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
package com.marklogic.mapreduce.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * Super class for wrapper classes for lexicon word and value match
 * functions, such as <code>cts:element-value-match</code> or 
 * <code>cts:field-word-match</code>.
 * 
 * @author jchen
 */
public abstract class ValueOrWordMatchFunction extends LexiconFunction {

    abstract void appendFunctionName(StringBuilder buf);
    
    abstract void appendNamesParams(StringBuilder buf);
    
    /**
     * Get the value for the $pattern parameter to a lexicon function call.
     * <p>
     *   This method corresponds to the $pattern parameter value of a 
     *   lexicon function call. Override this function to supply a
     *   pattern other than the default.
     * </p>
     * <p>
     *   When evaluated as XQuery, the returned string must resolve to
     *   an atomic type matching the type of the lexicon. String patterns
     *   may include wildcard characters.
     * </p>
     * 
     * @return the pattern to match.
     */
    public String getPattern() {
        return "*";
    }
    
    @Override
    public String getInputQuery(Collection<String> nsCol, long start, 
            long count) {
        long end = count == Long.MAX_VALUE ? count : start + count;
        StringBuilder buf = new StringBuilder();      
        
        buf.append("xquery version \"1.0-ml\"; \n");
        buf.append("xdmp:with-namespaces(("); 
        if (nsCol != null) {
            for (Iterator<String> nsIt = nsCol.iterator(); nsIt.hasNext();) {
                String ns = nsIt.next();
                buf.append('"').append(ns).append('"');
                if (nsIt.hasNext()) {
                    buf.append(',');
                }
            }
        }
        buf.append("),");
        // function name
        appendFunctionName(buf);
        buf.append("(");
        // names
        appendNamesParams(buf);
        // pattern
        buf.append(getPattern());
        buf.append(",");
        // options
        buf.append("(");
        String[] userOptions = getUserDefinedOptions();
        if (userOptions != null) {
            for (int i = 0; i < userOptions.length; i++) {
                if (i != 0) { 
                    buf.append(",\"");
                }
                buf.append("\"").append(userOptions[i]).append("\"");
            }          
        }
        buf.append("),");
        // query
        buf.append(getLexiconQuery()).append("))");
        // range 
        buf.append("[").append(start).append(" to "); 
        buf.append(end).append("]");
        return buf.toString();
    }
    
    public static void main(String[] args) { 
        UriMatch uriMatchFunc = new UriMatch();
        Collection<String> nsbindings = new ArrayList<String>();
        System.out.println(uriMatchFunc.getInputQuery(nsbindings, 1, 1000));
    }
}
