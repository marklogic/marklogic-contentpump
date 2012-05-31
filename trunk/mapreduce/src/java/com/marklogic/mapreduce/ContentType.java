package com.marklogic.mapreduce;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

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

        @Override
        public Class<? extends Writable> getWritableClass() {
            return Text.class;
        }
    },
    TEXT {
        public DocumentFormat getDocumentFormat() {
            return DocumentFormat.TEXT;
        }

        @Override
        public Class<? extends Writable> getWritableClass() {
            return Text.class;
        }
    },
    BINARY {
        public DocumentFormat getDocumentFormat() {
            return DocumentFormat.BINARY;
        }

        @Override
        public Class<? extends Writable> getWritableClass() {
            return BytesWritable.class;
        }
    },
    UNKNOWN {
        @Override
        public DocumentFormat getDocumentFormat() {
            return DocumentFormat.NONE;
        }

        @Override
        public Class<? extends Writable> getWritableClass() {
            return MarkLogicDocument.class;
        }       
    };
    
    public abstract DocumentFormat getDocumentFormat();
    
    public abstract Class<? extends Writable> getWritableClass();
    
    public static ContentType forName(String typeName) {
        if (typeName.equalsIgnoreCase(XML.name())) {
            return XML;
        } else if (typeName.equalsIgnoreCase(TEXT.name())) {
            return TEXT;
        } else if (typeName.equalsIgnoreCase(BINARY.name())) {
            return BINARY;
        } else if (typeName.equals(UNKNOWN)) {
            return UNKNOWN;
        } else {
            throw new IllegalArgumentException("Unknown content type: " + 
                    typeName);
        }
    }

    public static ContentType valueOf(int ordinal) {
        if (ordinal == 0) {
            return XML;
        } else if (ordinal == 1) {
            return TEXT;
        } else if (ordinal == 2) {
            return BINARY;
        } else if (ordinal == 3) {
            return UNKNOWN;
        }
        return null;
    }
}
