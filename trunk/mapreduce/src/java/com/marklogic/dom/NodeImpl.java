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
package com.marklogic.dom;

import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.UserDataHandler;

import com.marklogic.tree.ExpandedTree;
import com.marklogic.tree.NodeKind;
import com.sun.org.apache.xerces.internal.dom.TextImpl;

public abstract class NodeImpl implements Node {

    public static final boolean trace = false;

    private static final NodeList emptyNodeList = new NodeList() {
    
        public int getLength() {
            return 0;
        }

    
        public Node item(int index) {
            return null;
        }
    };

    final ExpandedTree tree;

    final int node;

    /**
     * No public constructor; only subclasses of Node should be instantiated
     */
    NodeImpl(ExpandedTree tree, int node) {
        this.tree = tree;
        this.node = node;
    }

    /** Constructor for serialization. */
//    public NodeImpl() {}


    public Node appendChild(Node newChild) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    // TODO - override in subclasses?
    public Node cloneNode(boolean deep) {
        return null;
    }

    public short compareDocumentPosition(Node other) throws DOMException {
        if (other instanceof NodeImpl) {
            NodeImpl otherNode = (NodeImpl)other;
            if (this.tree == otherNode.tree) {
                if (tree.nodeOrdinal[node] > tree.nodeOrdinal[otherNode.node]) {
                    return DOCUMENT_POSITION_PRECEDING;
                    // TODO
                    // return DOCUMENT_POSITION_CONTAINS;
                } else {
                    return DOCUMENT_POSITION_FOLLOWING;
                    // TODO
                    // return DOCUMENT_POSITION_CONTAINED_BY;
                }
            } else {
                return DOCUMENT_POSITION_DISCONNECTED;
            }
        } else {
            throw new DOMException(DOMException.NOT_SUPPORTED_ERR, null);
        }
    }


    public NamedNodeMap getAttributes() {
        return null;
    }

    // TODO
    public String getBaseURI() {
        return tree.getDocumentURI();
    }

    public NodeList getChildNodes() {
        return emptyNodeList;
    }

    // TODO
    public Object getFeature(String feature, String version) {
        assert (false);
        return this;
    }


    public Node getFirstChild() {
        return null;
    }


    public Node getLastChild() {
        return null;
    }


    public String getLocalName() {
        return null;
    }


    public String getNamespaceURI() {
        return null;
    }

    protected Node getNextChild(int node) {
        return null;
    }


    public Node getNextSibling() {
        NodeImpl p = (NodeImpl)getParentNode();
        return (p == null ? null : p.getNextChild(node));
    }


    public abstract String getNodeName();


    public short getNodeType() {
        return NodeKind.domType(tree.nodeKind[node]);
    }


    public String getNodeValue() throws DOMException {
        return null; // overridden in some subclasses
    }

    // TODO

    public Document getOwnerDocument() {
        assert (false);
        return null;
    }


    public Node getParentNode() {
        //assume no linkNodeKind
        return tree.node(tree.nodeParentNodeRepID[node]);
    }


    public String getPrefix() {
        return null;
    }

    protected Node getPreviousChild(int child) {
        return null;
    }


    public Node getPreviousSibling() {
        NodeImpl p = (NodeImpl)getParentNode();
        return (p == null ? null : p.getPreviousChild(node));
    }

    final boolean hasTextContent(Node child) {
        return child.getNodeType() != Node.COMMENT_NODE &&
            child.getNodeType() != Node.PROCESSING_INSTRUCTION_NODE &&
            (child.getNodeType() != Node.TEXT_NODE ||
             ((TextImpl) child).isIgnorableWhitespace() == false);
    }
    
    // TODO
    public String getTextContent() throws DOMException {
        return getNodeValue();
    }

    // TODO - is this allowed to throw NO_MODIFICATION_ALLOWED_ERR?
    public Object getUserData(String key) {
        assert (false);
        return null;
    }


    public boolean hasAttributes() {
        return false;
    }


    public boolean hasChildNodes() {
        return false;
    }


    public Node insertBefore(Node newChild, Node refChild) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    // TODO - override in subclasses?

    public boolean isDefaultNamespace(String namespaceURI) {
        assert (false);
        return false;
    }

    // TODO - override in subclasses?
    public boolean isEqualNode(Node other) {
        assert (false);
        return false;
    }


    public boolean isSameNode(Node other) {
        return (other instanceof NodeImpl) && (((NodeImpl)other).tree == tree) && (((NodeImpl)other).node == node);
    }

    // TODO: Consider implementing Traversal
    // TODO - override in subclasses?

    public boolean isSupported(String feature, String version) {
        if (feature.equalsIgnoreCase("Core"))
            return true;
        if (feature.equalsIgnoreCase("XML"))
            return true;
        return false;
    }

    // TODO - override in subclasses?

    public String lookupNamespaceURI(String prefix) {
        assert (false);
        return null;
    }

    // TODO - override in subclasses?

    public String lookupPrefix(String namespaceURI) {
        assert (false);
        return null;
    }


    public void normalize() {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }


    public Node removeChild(Node oldChild) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }


    public Node replaceChild(Node newChild, Node oldChild) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }


    public void setNodeValue(String nodeValue) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }


    public void setPrefix(String prefix) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }


    public void setTextContent(String textContent) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }


    public Object setUserData(String key, Object data, UserDataHandler handler) {
        throw new DOMException(DOMException.NOT_SUPPORTED_ERR, null);
    }
}
