/*
 * Copyright (c) 2003-2012 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce.functions;

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
        buf.append("(\"skip=").append(start);
        buf.append("\",\"truncate=").append(count).append("\"");
        String[] userOptions = getUserDefinedOptions();
        if (userOptions != null) {
            for (int i = 0; i < userOptions.length; i++) {
                buf.append(",\"").append(userOptions[i]).append("\"");
            }          
        }
        buf.append("),");
        // query
        buf.append(getLexiconQuery()).append("))");
        return buf.toString();
    }
}
