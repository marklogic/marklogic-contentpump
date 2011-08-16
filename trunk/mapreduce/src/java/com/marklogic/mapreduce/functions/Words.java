/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce.functions;

/**
 * Class to encapsulate input and generate query based on cts:words() function.
 * 
 * @author jchen
 */
public class Words extends ValuesOrWordsFunction {

    @Override
    void appendFunctionName(StringBuilder buf) {
        buf.append("cts:words");
    }

    @Override
    void appendNamesParams(StringBuilder buf) {      
    }
}
