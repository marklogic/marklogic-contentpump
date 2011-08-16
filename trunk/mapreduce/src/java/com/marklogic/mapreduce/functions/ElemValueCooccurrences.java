/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce.functions;

/**
 * Class to encapsulate input and generate query based on 
 * cts:element-value-co-occurrences() function.
 * 
 * @author jchen
 */
public abstract class ElemValueCooccurrences extends CooccurrencesFunction {

    /**
     * Get first element QName.
     * 
     * @return first element QName.
     */
    public abstract String getElementName1();
    
    /**
     * Get second element QName.
     * 
     * @return second element QName.
     */
    public abstract String getElementName2();
    
    @Override
    void appendFunctionName(StringBuilder buf) {
        buf.append("cts:element-value-co-occurrences");
    }
    
    @Override
    void appendNamesParams(StringBuilder buf) {
        // first element name   
        buf.append(getElementName1()).append(",\n");
        // second element name
        buf.append(getElementName2()).append(",\n");;
    }
}
