/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce.functions;

/**
 * Class to encapsulate input and generate query based on cts:uris() function.
 * 
 * @author jchen
 */
public class Uris extends ValuesOrWordsFunction {

    @Override
    void appendFunctionName(StringBuilder buf) {
        buf.append("cts:uris");    
    }

    @Override
    void appendNamesParams(StringBuilder buf) {        
    }

}
