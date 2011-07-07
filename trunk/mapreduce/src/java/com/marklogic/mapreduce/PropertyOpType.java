package com.marklogic.mapreduce;

/**
 * Type of supported property operations.
 * 
 * <p>
 *  When using {@link PropertyOutputFormat}, set the configuration property
 *  <code>mapreduce.marklogic.output.propertyoptype</code> to
 *  one of these values to control how the output property value is 
 *  handled by the server.
 * </p>
 * <p>
 *  Use <code>SET_PROPERTY</code> to replace any existing properties with
 *  the new property. Use <code>ADD_PROPERTY</code> to add a property
 *  without removing existing properties.
 * </p>
 * <p>
 *  For more information, see the following built-in functions in the
 *  <em>XQuery & XSLT API Reference</em>:
 *  <ul>
 *   <li>xdmp:document-set-property</li>
 *   <li>xdmp:document-add-properties</li>
 *  </ul>
 * </p>
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
