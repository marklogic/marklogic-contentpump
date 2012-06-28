/*
 * Copyright (c) 2003-2012 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

/*
 * Enum to capture indentation config setting.
 */
public enum Indentation {
    TRUE {
        @Override
        public String getStatement() {
            return "declare option xdmp:output \"indent=yes\";" +
            "declare option xdmp:output \"indent-untyped=yes\";";
        }
    },
    FALSE {
        @Override
        public String getStatement() {
            return "declare option xdmp:output \"indent=no\";" +
            "declare option xdmp:output \"indent-untyped=no\";";
        }
    },
    SERVERDEFAULT {
        @Override
        public String getStatement() {
            return "";
        }
    };
    
    public abstract String getStatement();
    public static Indentation forName(String indent) {
        if (indent.equalsIgnoreCase("TRUE")) {
            return Indentation.TRUE;
        } else if (indent.equalsIgnoreCase("FALSE")) {
            return Indentation.FALSE;
        } else if (indent.equalsIgnoreCase("SERVERDEFAULT")) {
            return Indentation.SERVERDEFAULT;
        } else {
            throw new IllegalArgumentException("Unknown indentation setting: " 
                         + indent);
        }
    }
}
