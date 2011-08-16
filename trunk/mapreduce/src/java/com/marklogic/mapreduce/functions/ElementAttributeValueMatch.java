/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce.functions;

/**
 * Class to encapsulate input and generate query based on 
 * cts:element-attribute-value-match() function.
 * 
 * @author jchen
 */
public abstract class ElementAttributeValueMatch extends
        ValueOrWordMatchFunction {

    /**
     * Get an array of element QNames to be used with the function.
     * 
     * @return an array of element QNames.
     */
    public abstract String[] getElementNames();
    
    /**
     * Get an array of element QNames to be used with the function.
     * 
     * @return an array of element QNames.
     */
    public abstract String[] getAttributeNames();
    
    void appendFunctionName(StringBuilder buf) {
        buf.append("cts:element-attribute-value-match");
    }
    
    void appendNamesParams(StringBuilder buf) {
        // element names
        buf.append("(");
        String[] elemNames = getElementNames();
        for (int i = 0; i < elemNames.length; i++) {
            if (i > 0) {
                buf.append(",");
            }
            buf.append(elemNames[i]);
        }
        buf.append("),(");
        // attribute names
        String[] attrNames = getAttributeNames();
        for (int i = 0; i < attrNames.length; i++) {
            if (i > 0) {
                buf.append(",");
            }
            buf.append(attrNames[i]);
        }
        buf.append("),\n");
    }
}
