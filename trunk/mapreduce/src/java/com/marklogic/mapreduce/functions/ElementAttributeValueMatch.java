/*
 * Copyright (c) 2003-2012 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce.functions;

/**
 * Wrapper class for the <code>cts:element-attribute-value-match</code>
 *  lexicon function. Subclass this class to generate map input based on a lexicon.
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
public abstract class ElementAttributeValueMatch extends
        ValueOrWordMatchFunction {

    /**
     * Get the value of the $element-names parameter to the lexicon
     * function call, as an array of element QName strings.
     * <p>
     *   Each string in the array must evaluate to an xs:QName when 
     *   evaluated as XQuery. For example: "xs:QName(\"wp:a\")". 
     * </p>
     * 
     * @return an array of element QNames.
     */
    public abstract String[] getElementNames();
    
    /**
     * Get the value of the $attribute-names parameter to the lexicon
     * function call, as an array of attribute QName strings.
     * <p>
     *   Each string in the array must evaluate to an xs:QName when 
     *   evaluated as XQuery. For example: "xs:QName(\"wp:a\")". 
     * </p>
     * 
     * @return an array of attribute QNames.
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
