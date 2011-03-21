/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

/**
 * Type of supported node operations.
 * 
 * @author jchen
 */
public enum NodeOpType {
	INSERT_BEFORE {
    	public String getFunctionName() {
    		return "xdmp:node-insert-before";
    	}
    },
    INSERT_AFTER {
    	public String getFunctionName() {
    		return "xdmp:node-insert-after";
    	}
    },
    INSERT_CHILD {
    	public String getFunctionName() {
    		return "xdmp:node-insert-child";
    	}
    },
    REPLACE {
    	public String getFunctionName() {
    		return "xdmp:node-replace";
    	}
    };
    
    abstract public String getFunctionName();
    
    public String getQueryTemplate() {
    	StringBuilder buf = new StringBuilder();
    	buf.append("xquery version \"1.0-ml\"; \n");
    	buf.append("xdmp:with-namespaces((");
    	buf.append(MarkLogicConstants.NAMESPACE_TEMPLATE);
    	buf.append("),");
    	buf.append(getFunctionName());
    	buf.append("(");
    	buf.append(MarkLogicConstants.NODE_PATH_TEMPLATE);
    	buf.append(",");
    	buf.append(MarkLogicConstants.NODE_STRING_TEMPLATE);
    	buf.append("))");
    	return buf.toString(); 
    }
}
