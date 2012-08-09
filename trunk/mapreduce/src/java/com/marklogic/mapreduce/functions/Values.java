/*
 * Copyright (c) 2003-2012 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce.functions;

/**
 * Wrapper class for the <code>cts:values</code> lexicon function.
 * Subclass this class to generate map input based on a lexicon.
 * 
 * <p>
 *   Use this class with {@link com.marklogic.mapreduce.ValueInputFormat}.
 * </p>
 * <p>
 *   To generate map input using the lexicon function,
 *   create a subclass of this class and provide implementations
 *   of the methods that correspond to the function parameters you
 *   want to include in the call.
 * </p>
 * <p>
 *   For details, see "Using a Lexicon to Generate Key-Value Pairs"
 *   in the <em>MarkLogic Connector for Hadoop Developer's Guide</em>.
 * </p>
 * 
 * @author jchen
 */
public abstract class Values extends ValuesOrWordsFunction {

    /**
     * Get the value of the $range-indexes parameter to the lexicon
     * function call, as an array of References.
     * 
     * @return an array of references.
     */
    public abstract Reference[] getReferences();
    
    @Override
    void appendFunctionName(StringBuilder buf) {
        buf.append("cts:values");
    }
    
    @Override
    void appendNamesParams(StringBuilder buf) {
        Reference[] refs = getReferences();
        buf.append("(");
        for (int i = 0; i < refs.length; i++) {
            if (i > 0) {
                buf.append(",");
            }
            refs[i].append(buf);
        }
        buf.append("),\n");
    }
}
