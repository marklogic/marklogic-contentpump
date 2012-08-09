/*
 * Copyright (c) 2003-2012 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce.functions;

/**
 * Super class for all supported reference functions.
 * 
 * @author jchen
 */
public abstract class Reference {
    public abstract void append(StringBuilder buf);
}
