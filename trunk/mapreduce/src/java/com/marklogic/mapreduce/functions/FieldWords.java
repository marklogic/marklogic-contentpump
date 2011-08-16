/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce.functions;

/**
 * Class to encapsulate input and generate query based on cts:field-words() 
 * function.
 * 
 * @author jchen
 */
public abstract class FieldWords extends ValuesOrWordsFunction {

    /**
     * Get an array of field names to be used with the function.
     * 
     * @return an array of field names.
     */
    public abstract String[] getFieldNames();
    
    @Override
    void appendFunctionName(StringBuilder buf) {
        buf.append("cts:field-words");
    }
    
    @Override
    void appendNamesParams(StringBuilder buf) {
        String[] elemNames = getFieldNames();
        buf.append("(");
        for (int i = 0; i < elemNames.length; i++) {
            if (i > 0) {
                buf.append(",");
            }
            buf.append(elemNames[i]);
        }
        buf.append("),\n");
    }
}
