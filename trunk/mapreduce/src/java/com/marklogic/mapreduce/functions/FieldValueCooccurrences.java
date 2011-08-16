/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce.functions;

/**
 * Class to encapsulate input and generate query based on 
 * cts:field-value-co-occurrences() function.
 * 
 * @author jchen
 */
public abstract class FieldValueCooccurrences extends CooccurrencesFunction {

    /**
     * Get first field name.
     * 
     * @return first field name.
     */
    public abstract String getFieldName1();
    
    /**
     * Get second field name.
     * 
     * @return second field name.
     */
    public abstract String getFieldName2();
    
    @Override
    void appendFunctionName(StringBuilder buf) {
        buf.append("cts:field-value-co-occurrences");
    }
    
    @Override
    void appendNamesParams(StringBuilder buf) {
        // first element name   
        buf.append(getFieldName1()).append(",\n");
        // second element name
        buf.append(getFieldName2()).append(",\n");;
    }
 }
