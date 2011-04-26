package com.marklogic.mapreduce;

import com.marklogic.xcc.DocumentFormat;

/**
 * Type of supported document format.
 * 
 * @author jchen
 */
public enum ContentType {
    XML {
    	public DocumentFormat getDocumentFormat() {
    		return DocumentFormat.XML;
    	}
    },
    TEXT {
    	public DocumentFormat getDocumentFormat() {
    		return DocumentFormat.TEXT;
    	}
    },
    BINARY {
    	public DocumentFormat getDocumentFormat() {
    		return DocumentFormat.BINARY;
    	}
    };
    
    public abstract DocumentFormat getDocumentFormat();
}
