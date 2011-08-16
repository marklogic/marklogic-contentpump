/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce.functions;

/**
 * Class to encapsulate input and generate query based on 
 * cts:element-attribute-value-co-occurrences() function.
 * 
 * @author jchen
 */
public abstract class ElemAttrValueCooccurrences 
extends CooccurrencesFunction {

    /**
     * Get first element QName.
     * 
     * @return first element QName.
     */
    public abstract String getElementName1();
    
    /**
     * Get first attribute QName or empty sequence. The empty sequence 
     * specifies an element lexicon. 
     * 
     * @return first attribute QName.
     */
    public String getAttributeName1() {
        return "()";
    }
    
    /**
     * Get second element QName.
     * 
     * @return second element QName.
     */
    public abstract String getElementName2();
    
    /**
     * Get second attribute QName or empty sequence. The empty sequence 
     * specifies an element lexicon. 
     * 
     * @return second attribute QName.
     */
    public String getAttributeName2() {
        return "()";
    }
    
    @Override
    void appendFunctionName(StringBuilder buf) {
        buf.append("cts:element-attribute-value-co-occurrences");
    }
    
    @Override
    void appendNamesParams(StringBuilder buf) {
        // first element name   
        buf.append(getElementName1()).append(",\n");
        // first attribute name
        buf.append(getAttributeName1()).append(",\n");
        // second element name
        buf.append(getElementName2()).append(",\n");
        // second attribute name
        buf.append(getAttributeName2()).append(",\n");
    }
}
