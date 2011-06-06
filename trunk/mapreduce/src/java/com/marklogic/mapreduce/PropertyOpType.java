package com.marklogic.mapreduce;

/**
 * Type of supported property operations.
 * 
 * @author jchen
 */

public enum PropertyOpType {
    SET_PROPERTY {
        public String getFunctionName() {
            return "xdmp:document-set-property";
        }
    },
    ADD_PROPERTY {
        public String getFunctionName() {
            return "xdmp:document-add-property";
        }
    };
    
    abstract public String getFunctionName();
    
    public String getQuery(DocumentURI uri, String recordString) {
        StringBuilder buf = new StringBuilder();
        buf.append("xquery version \"1.0-ml\"; \n");
        buf.append(getFunctionName());
        buf.append("(\n\"");
        buf.append(uri.getUnparsedUri());
        buf.append("\", ");
        buf.append(recordString);
        buf.append(")");
        return buf.toString();
    }
}
