/*
 * Copyright 2003-2013 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
