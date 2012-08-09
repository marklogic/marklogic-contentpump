/*
 * Copyright (c) 2003-2012 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce.functions;

/**
 * Wrapper class for the <code>cts:value-co-occurrences</code> lexicon
 * function. Subclass this class to generate map input based on a lexicon.
 * 
 * <p>
 *   Use this class with {@link com.marklogic.mapreduce.KeyValueInputFormat}
 *   or {@link com.marklogic.mapreduce.ValueInputFormat}.
 * </p>
 * <p>
 *   To generate map input with the lexicon function,
 *   create a subclass of this class and provide implementations
 *   of the methods that correspond to the function parameters you
 *   want to include in the call.
 * </p>
 * <p>
 *   For details, see "Using a Lexicon to Generate Key-Value Pairs"
 *   in the <em>MarkLogic Connector for Hadoop Developer's Guide</em>.
 *   For an example, see
 *   {@link com.marklogic.mapreduce.examples.LinkCountCooccurrences}.
 * </p>
 * 
 * <p>
 *   All co-occurrences functions using this API do NOT preserve 
 *   frequency order even if "frequency-order" option is specified.
 * </p>
 * 
 * @author jchen
 */
public abstract class ValueCooccurrences extends CooccurrencesFunction {

    /**
     * Get the value for $range-index-1 parameter to the
     * lexicon function call, as a reference. 
     * 
     * @return first range index reference.
     */
    public abstract Reference getReference1();
    
    /**
     * Get the value for $range-index-2 parameter to the
     * lexicon function call, as a reference.
     * 
     * @return second range index reference.
     */
    public abstract Reference getReference2();
    
    @Override
    void appendFunctionName(StringBuilder buf) {
        buf.append("cts:value-co-occurrences");
    }
    
    @Override
    void appendNamesParams(StringBuilder buf) {
        // first element name   
        getReference1().append(buf);
        buf.append(",\n");
        // second element name
        getReference2().append(buf);
        buf.append(",\n");;
    }
 }
