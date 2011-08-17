/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce.functions;

import java.util.Collection;

/**
 * Super class for all supported lexicon functions.  MarkLogicInputFormat and
 * MarkLogicRecordReader get the input query and split query from these classes
 * when a lexicon function is specified in the config.
 * 
 * @author jchen
 */
public abstract class LexiconFunction {
    
    /**
     * Get the cts:query specified by the user as part of the lexicon function
     * to filter fragments used to retrieve the lexicons.
     * 
     * @return cts:query.
     */
    public String getLexiconQuery() {
        return "cts:and-query(())";
    }
    
    /**
     * Get the input query used by a record reader.
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
     */
    public String[] getUserDefinedOptions() {
        return null;
    }
}
