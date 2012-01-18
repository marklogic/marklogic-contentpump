/*
 * Copyright (c) 2003-2012 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce.functions;

import java.util.Collection;

/**
 * Super class for all supported lexicon functions. 
 * {@link com.marklogic.mapreduce.MarkLogicInputFormat} and
 * {@link com.marklogic.mapreduce.MarkLogicRecordReader} get the input query and 
 * split query from these classes when a lexicon function is specified in the 
 * config by setting
 * {@link com.marklogic.mapreduce.MarkLogicConstants#INPUT_LEXICON_FUNCTION_CLASS 
 * input.lexiconfunctionclass}.
 * 
 * @author jchen
 */
public abstract class LexiconFunction {
    
    /**
     * Get the cts:query specified by the user as part of the lexicon function
     * to filter fragments used to retrieve the lexicons. 
     * <p>
     *   This corresponds to the $query parameter of a lexicon function. Users 
     *   may override this method if the default is not suitable.
     * </p>
     * 
     * @return cts:query.
     */
    public String getLexiconQuery() {
        return "()";
    }
    
    /**
     * Get the input query used by a record reader. Users must not override
     * this method.
     * @param nsCol alias-URI pairs of namespace specs
     * @param start start of the split
     * @param count count of the split
     * 
     * @return input query.
     */
    public abstract String getInputQuery(Collection<String> nsCol, 
            long start, long count);
    
    /**
     * Get user-defined options for the lexicon function.
     * <p>
     *   This method corresponds to the $options parameter of a lexicon function.
     *   Override this method to pass options to the lexicon function. The
     *   options <em>skip</em> and <em>truncate</em> are reserved for internal
     *   use by the connector. 
     * </p>
     */
    public String[] getUserDefinedOptions() {
        return null;
    }
}
