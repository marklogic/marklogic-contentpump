/*
 * Copyright (c) 2003-2012 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce.functions;

/**
 * Wrapper class for the <code>cts:path-reference</code> function.
 * This class represents a reference to a path value lexicon
 * (path range index).
 * 
 * @author jchen
 */
public abstract class PathReference extends Reference {

    public void append(StringBuilder buf) {
        buf.append("cts:path-reference(\"");
        buf.append(getPathExpression());
        buf.append("\"");
        String[] userOptions = getUserDefinedOptions();
        if (userOptions != null) {
            buf.append(",");
            for (int i = 0; i < userOptions.length; i++) {
                buf.append(",\"").append(userOptions[i]).append("\"");
            }          
        }
        buf.append(")");    
    }
    
    /**
     * The path range expression represented by this lexicon.
     * <p>
     *   This path expression must correspond to the path expression
     *   used to create an existing path range index.
     * </p>
     * @return
     */
    public abstract String getPathExpression();
    
    /**
     * Get user-defined options for the path reference.
     * <p>
     *   This method corresponds to the $options parameter of the path
     *   reference function.  Override this method to pass options to the path 
     *   reference function. For a list of available options, see 
     *   <code>cts:path-reference</code> in the <em>XQuery and XSLT Reference</em>.
     * </p>
     */
    public String[] getUserDefinedOptions() {
        return null;
    }
}
