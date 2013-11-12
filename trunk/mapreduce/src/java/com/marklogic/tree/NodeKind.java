package com.marklogic.tree;

public class NodeKind {

    public static final byte ELEM = 0;
    public static final byte ATTR = 1;
    public static final byte TEXT = 2;
    public static final byte LINK = 3;
    public static final byte NS = 4;
    public static final byte DOC = 5;
    public static final byte PI = 6;
    public static final byte COMMENT = 7;
    public static final byte PERM = 8;
    public static final byte BINARY = 9;
    public static final byte NULL = 10;
    
    public static final short domType(byte kind) {
        switch (kind) {
        case ELEM:
        case ATTR:
        case TEXT:
        case PI:
        case COMMENT:
            return (short)(kind+1);
        case DOC:
            return 9;
        default:
            return 0;
        }
    }
}
