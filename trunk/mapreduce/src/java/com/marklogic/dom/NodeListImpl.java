package com.marklogic.dom;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class NodeListImpl implements NodeList {

    private static class Empty implements NodeList {

        @Override
        public int getLength() {
            return 0;
        }

        @Override
        public Node item(int arg0) {
            return null;
        }
    }

    public static final NodeList EMPTY = new Empty();

    @Override
    public int getLength() {
        return 0;
    }

    @Override
    public Node item(int arg0) {
        return null;
    }
}
