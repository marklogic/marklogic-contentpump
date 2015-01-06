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
package com.marklogic.mapreduce.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * Super class for wrapper classes for supported values and words
 * lexicon functions, such as <code>cts:element-values</code>.
 * 
 * @author jchen
 */
public abstract class ValuesOrWordsFunction extends LexiconFunction {

    /**
     * Get the starting value for the function.
     * <p>
     *   This method corresponds to the $start parameter value of
     *   a lexicon function. Override the method to specify a
     *   lexicon starting position other than the default.  
     * </p>
     * <p>
     *   The returned value must have the expected type when
     *   evaluated as XQuery. Therefore, if the data type of the
     *   start value is xs:string, the returned string must include
     *   escaped double quotes. For example, if the start value 
     *   should be the string "foo" in XQuery, return "\"foo\"".
     * </p>
     * 
     * @return the starting value for the function.
     */
    public String getStart() {
        return "()";
    }
    
    abstract void appendFunctionName(StringBuilder buf);
    
    abstract void appendNamesParams(StringBuilder buf);
    
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
        // start
        buf.append(getStart());
        // option
        buf.append(",(");
        String[] userOptions = getUserDefinedOptions();
        if (userOptions != null) {           
            for (int i = 0; i < userOptions.length; i++) {
                if (i != 0) {
                    buf.append(",");
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
        Words wordsFunc = new WordsFunction();
        Collection<String> nsbindings = new ArrayList<String>();
        for (int i = 0; i < args.length; i++) {
            nsbindings.add(args[i]);
        }
        System.out.println(wordsFunc.getInputQuery(nsbindings, 1, 1000));
    }
    
    static class WordsFunction extends Words {
        public String[] getUserDefinedOptions() {
            String[] options =
                {"document","concurrent"};
            return options;
        }

    }

}
