/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce.functions;

/**
 * Class to encapsulate input and generate query based on 
 * cts:collection-match() function.
 * 
 * @author jchen
 */
public class CollectionMatch extends ValueOrWordMatchFunction {

    @Override
    void appendFunctionName(StringBuilder buf) {
        buf.append("cts:collection-match");
    }

    @Override
    void appendNamesParams(StringBuilder buf) {
    }

}
