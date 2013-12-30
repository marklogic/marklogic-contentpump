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

import java.util.Collection;
import java.util.Iterator;

/**
 * Supper class of wrapper classes for supported co-occurrences lexicon
 * functions, such as <code>cts:element-value-co-occurrences</code>.
 * 
 * <p>
 *   All co-occurrences functions using this API do NOT preserve 
 *   frequency order even if "frequency-order" option is specified.
 * </p>
 * 
 * @author jchen
 */
public abstract class CooccurrencesFunction extends LexiconFunction {

    abstract void appendFunctionName(StringBuilder buf);
    
    abstract void appendNamesParams(StringBuilder buf);
    
    @Override
    public String getInputQuery(Collection<String> nsCol, long start, 
            long count) {
        StringBuilder buf = new StringBuilder();      
        
        buf.append("xquery version \"1.0-ml\";\n");
        buf.append("let $M := xdmp:with-namespaces((");
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
        // options
        buf.append("(\"skip=").append(start);
        buf.append("\",\"truncate=").append(count).append("\"");
        buf.append(",\"map\"");
        String[] userOptions = getUserDefinedOptions();
        if (userOptions != null) {
            for (int i = 0; i < userOptions.length; i++) {
                buf.append(",\"").append(userOptions[i]).append("\"");
            }          
        }
        buf.append("),\n");
        // query
        buf.append(getLexiconQuery()).append("))\n");
        buf.append("for $k in map:keys($M)\n");
        buf.append("let $v := map:get($M, $k)\n");
        buf.append("for $each in $v\n");
        buf.append("return ($k, $each)\n");

        return buf.toString();
    }
}
