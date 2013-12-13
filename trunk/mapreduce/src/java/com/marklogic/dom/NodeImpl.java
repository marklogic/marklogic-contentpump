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

public class NodeImpl implements Node {

    public static final boolean trace = false;

    private static final NodeList emptyNodeList = new NodeList() {
        @Override
        public int getLength() {
            return 0;
        }

        @Override
        public Node item(int index) {
            return null;
        }
    };

    final ExpandedTree tree;

    final int node;

    NodeImpl(ExpandedTree tree, int node) {
        this.tree = tree;
        this.node = node;
    }

    @Override
    public Node appendChild(Node newChild) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    // TODO - override in subclasses?
    @Override
    public Node cloneNode(boolean deep) {
        return null;
    }

    @Override
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

    @Override
    public NamedNodeMap getAttributes() {
        return null;
    }

    // TODO
    @Override
    public String getBaseURI() {
        return tree.getDocumentURI();
    }

    @Override
    public NodeList getChildNodes() {
        return emptyNodeList;
    }

    // TODO
    @Override
    public Object getFeature(String feature, String version) {
        assert (false);
        return this;
    }

    @Override
    public Node getFirstChild() {
        return null;
    }

    @Override
    public Node getLastChild() {
        return null;
    }

    @Override
    public String getLocalName() {
        return null;
    }

    @Override
    public String getNamespaceURI() {
        return null;
    }

    protected Node getNextChild(int node) {
        return null;
    }

    @Override
    public Node getNextSibling() {
        NodeImpl p = (NodeImpl)getParentNode();
        return (p == null ? null : p.getNextChild(node));
    }

    @Override
    public String getNodeName() {
        return null;
    }

    @Override
    public short getNodeType() {
        return NodeKind.domType(tree.nodeKind[node]);
    }

    @Override
    public String getNodeValue() throws DOMException {
        return null;
    }

    // TODO
    @Override
    public Document getOwnerDocument() {
        assert (false);
        return null;
    }

    @Override
    public Node getParentNode() {
        return tree.node(tree.nodeParentNodeRepID[node]);
    }

    @Override
    public String getPrefix() {
        return null;
    }

    protected Node getPreviousChild(int child) {
        return null;
    }

    @Override
    public Node getPreviousSibling() {
        NodeImpl p = (NodeImpl)getParentNode();
        return (p == null ? null : p.getPreviousChild(node));
    }

    // TODO
    @Override
    public String getTextContent() throws DOMException {
        assert (false);
        return null;
    }

    // TODO - is this allowed to throw NO_MODIFICATION_ALLOWED_ERR?
    @Override
    public Object getUserData(String key) {
        assert (false);
        return null;
    }

    @Override
    public boolean hasAttributes() {
        return false;
    }

    @Override
    public boolean hasChildNodes() {
        return false;
    }

    @Override
    public Node insertBefore(Node newChild, Node refChild) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    // TODO - override in subclasses?
    @Override
    public boolean isDefaultNamespace(String namespaceURI) {
        assert (false);
        return false;
    }

    // TODO - override in subclasses?
    public boolean isEqualNode(Node other) {
        assert (false);
        return false;
    }

    @Override
    public boolean isSameNode(Node other) {
        return (other instanceof NodeImpl) && (((NodeImpl)other).tree == tree) && (((NodeImpl)other).node == node);
    }

    // TODO: Consider implementing Traversal
    // TODO - override in subclasses?
    @Override
    public boolean isSupported(String feature, String version) {
        if (feature.equalsIgnoreCase("Core"))
            return true;
        if (feature.equalsIgnoreCase("XML"))
            return true;
        return false;
    }

    // TODO - override in subclasses?
    @Override
    public String lookupNamespaceURI(String prefix) {
        assert (false);
        return null;
    }

    // TODO - override in subclasses?
    @Override
    public String lookupPrefix(String namespaceURI) {
        assert (false);
        return null;
    }

    @Override
    public void normalize() {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    @Override
    public Node removeChild(Node oldChild) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    @Override
    public Node replaceChild(Node newChild, Node oldChild) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    @Override
    public void setNodeValue(String nodeValue) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    @Override
    public void setPrefix(String prefix) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    @Override
    public void setTextContent(String textContent) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    @Override
    public Object setUserData(String key, Object data, UserDataHandler handler) {
        throw new DOMException(DOMException.NOT_SUPPORTED_ERR, null);
    }
}
