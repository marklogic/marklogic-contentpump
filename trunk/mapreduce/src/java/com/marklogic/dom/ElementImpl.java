/*
 * Copyright 2003-2014 MarkLogic Corporation
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

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Attr;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.TypeInfo;

import com.marklogic.tree.ExpandedTree;
import com.marklogic.tree.NodeKind;

/**
 * A read-only W3C DOM Node implementation of MarkLogic's internal
 * representation of a document element as stored in the expanded tree cache of
 * a forest on disk.
 * 
 * <p>
 * This interface is effectively read-only: Setters and update methods inherited
 * from <code>org.w3c.Node</code> are not supported and will raise an exception
 * if called.
 * </p>
 * 
 * @author jchen
 */
public class ElementImpl extends NodeImpl implements Element {
    public static final Log LOG = LogFactory.getLog(ElementImpl.class);
    protected AttributeNodeMapImpl attributes;
    protected int numNSDecl;

    public ElementImpl(ExpandedTree tree, int node) {
        super(tree, node);
        attributes = new AttributeNodeMapImpl(this);
        numNSDecl = -1;
    }

    protected Node cloneNode(Document doc, boolean deep) {
        Element elem = doc.createElementNS(getNamespaceURI(), getTagName());
        elem.setPrefix(getPrefix());

        for (int i = 0; i < attributes.getLength(); i++) {
            Attr attr = (Attr) attributes.item(i);
            if (attr instanceof AttrImpl) {
                elem.setAttributeNode((Attr) ((AttrImpl) attr).cloneNode(doc,
                    deep));
            } else {
                // ns decl, stored as Java DOM Attr
                // uri of xmlns is "http://www.w3.org/2000/xmlns/"
                Attr clonedAttr = doc.createAttributeNS(
                    "http://www.w3.org/2000/xmlns/", attr.getName());
                clonedAttr.setValue(attr.getValue());
                elem.setAttributeNode(clonedAttr);
            }
        }

        if (deep) {
            // clone children
            NodeList list = getChildNodes();
            for (int i = 0; i < list.getLength(); i++) {
                NodeImpl n = (NodeImpl) list.item(i);
                Node c = n.cloneNode(doc, true);
                elem.appendChild(c);
            }
        }
        return elem;
    }

    /** {@inheritDoc} */
    public String getAttribute(String name) {
    	Attr attr = getAttributeNode(name);
        return attr==null?"":attr.getValue();
    }

    /** {@inheritDoc} */
    public Attr getAttributeNode(String name) {
        return (attributes==null)?null:(Attr)(attributes.getNamedItem(name));
    }

    /** {@inheritDoc} */
    public Attr getAttributeNodeNS(String namespaceURI, String localName){
        return (attributes==null)?null:(AttrImpl)getAttributes().getNamedItemNS(namespaceURI,
            localName);
    }

    /** {@inheritDoc} */
    public String getAttributeNS(String namespaceURI, String localName) {
    	if (attributes==null) return "";
    	AttrImpl attr = 
    		(AttrImpl) getAttributes().getNamedItemNS(namespaceURI,localName);
        return attr==null?"":attr.getValue();
    }

    
    /**
     * {@inheritDoc}
     * <P>
     * Attributes returned contains default attributes enforced by XML Schema in MarkLogic.
     * 
     */
    @Override
    public NamedNodeMap getAttributes() {
        return attributes;
    }

    /** {@inheritDoc} */
    @Override
    public NodeList getChildNodes() {
        return new NodeList() {
            public int getLength() {
                return tree.elemNodeNumChildren[tree.nodeRepID[node]];
            }

            public Node item(int index) {
                return (index < getLength()) ? tree
                    .node(tree.elemNodeChildNodeRepID[tree.nodeRepID[node]]
                        + index) : null;
            }
        };
    }

    /** {@inheritDoc} */
    @Override
    public boolean isDefaultNamespace(String namespaceURI) {
        String namespace = this.getNamespaceURI();
        String prefix = this.getPrefix();

        if (prefix == null || prefix.length() == 0) {
            if (namespaceURI == null) {
                return (namespace == namespaceURI);
            }
            return namespaceURI.equals(namespace);
        }
        if (this.hasAttributes()) {
            Attr attr = this.getAttributeNodeNS(
                "http://www.w3.org/2000/xmlns/", "xmlns");
            if (attr != null) {
                String value = attr.getNodeValue();
                if (namespaceURI == null) {
                    return (namespace == value);
                }
                return namespaceURI.equals(value);
            }
        }

        Node ancestor = getParentNode();
        if (ancestor != null) {
            short type = ancestor.getNodeType();
            if (type == NodeKind.ELEM) {
                return ancestor.isDefaultNamespace(namespaceURI);
            }
            // otherwise, current node is root already
        }
        return false;
    }

    /** {@inheritDoc} */
    public NodeList getElementsByTagNameNS(String namespaceURI, String name) {
        return getElementsByTagNameNSOrNodeName(namespaceURI, name, false);
    }

    /** {@inheritDoc} */
    public NodeList getElementsByTagName(String localName) {
        return getElementsByTagNameNSOrNodeName(null, localName, true);
    }

    protected int getNumChildren() {
        return tree.elemNodeNumChildren[tree.nodeRepID[node]];
    }

    protected int getFirstChildIndex() {
        return tree.elemNodeChildNodeRepID[tree.nodeRepID[node]];
    }

    public Node getFirstChild() {
        return tree.node(getFirstChildIndex());
    }

    @Override
    public Node getLastChild() {
        return tree.node(tree.elemNodeChildNodeRepID[tree.nodeRepID[node]]
            + tree.elemNodeNumChildren[tree.nodeRepID[node]] - 1);
    }

    @Override
    public String getLocalName() {
        return tree
            .atomString(tree.nodeNameNameAtom[tree.elemNodeNodeNameRepID[tree.nodeRepID[node]]]);
    }

    @Override
    public String getNamespaceURI() {
        return tree
            .atomString(tree.nodeNameNamespaceAtom[tree.elemNodeNodeNameRepID[tree.nodeRepID[node]]]);
    }

    @Override
    public Node getNextChild(int child) {
        return (child - tree.elemNodeChildNodeRepID[tree.nodeRepID[node]] + 1 < tree.elemNodeNumChildren[tree.nodeRepID[node]]) ? tree
            .node(child + 1) : null;
    }

    @Override
    public String getNodeName() {
        String ns = getPrefix();
        return ns == null || ns.equals("") ? getTagName() : ns + ":"
            + getTagName();
    }

    protected int getPrefixID(int uriAtom) {
        int a = -1;
        boolean useDefaultNS = true;
        ArrayList<Integer> ubp = new ArrayList<Integer>();
        long minOrdinal = 0;
        for (int ns = getNSNodeID(tree.nodeOrdinal[node]); ns >= 0; ns = nextNSNodeID(
            ns, minOrdinal)) {
            int uri = tree.nsNodeUriAtom[ns];
            int prefix = tree.nsNodePrefixAtom[ns];
            if (tree.atomString(uri) == null) {
                ubp.add(prefix);
                continue;
            }
            if (uri != uriAtom) {
                useDefaultNS &= (tree.atomString(prefix) != null);
                continue;
            }
            if (ubp.contains(prefix))
                continue;
            if (tree.atomString(prefix) != null) {
                if (a == -1)
                    a = prefix;
                continue;
            }
            if (useDefaultNS)
                return prefix;
        }
        return a;
    }

    public int getNumNSDecl() {
        if (numNSDecl != -1)
            return numNSDecl;
        numNSDecl = 0;
        long minOrdinal = tree.nodeOrdinal[node];
        for (int ns = getNSNodeID(minOrdinal, minOrdinal); ns >= 0; ns = nextNSNodeID(
            ns, minOrdinal)) {
            numNSDecl++;
        }
        return numNSDecl;
    }

    @Override
    public String getPrefix() {
        int ns = tree.nodeNameNamespaceAtom[tree.elemNodeNodeNameRepID[tree.nodeRepID[node]]];
        if (ns < 0)
            return null;
        String preserved = builtinNSPrefix(getNamespaceURI());
        if (preserved != null)
            return preserved;
        if (tree.atomString(ns) != null)
            ns = getPrefixID(ns);
        String r = (ns >= 0) ? tree.atomString(ns) : null;
        return r;
    }

    @Override
    protected Node getPreviousChild(int child) {
        return (child != tree.elemNodeChildNodeRepID[tree.nodeRepID[node]]) ? tree
            .node(child - 1) : null;
    }

    public TypeInfo getSchemaTypeInfo() {
        return null;
    }

    public String getTagName() {
        return tree
            .atomString(tree.nodeNameNameAtom[tree.elemNodeNodeNameRepID[tree.nodeRepID[node]]]);
    }

    public boolean hasAttribute(String name) {
        return (getAttributes().getNamedItem(name) != null);
    }

    public boolean hasAttributeNS(String namespaceURI, String localName)
        throws DOMException {
        return (getAttributes().getNamedItemNS(namespaceURI, localName) != null);
    }

    public boolean hasAttributes() {
        return getAttributes().getLength() > 0;
    }

    @Override
    public boolean hasChildNodes() {
        return getChildNodes().getLength() > 0;
    }

    // The following page gives algorithm for the following functions
    // http://www.w3.org/TR/DOM-Level-3-Core/namespaces-algorithms.html#lookupNamespacePrefixAlgo

    @Override
    public String lookupNamespaceURI(String prefix) {
        if (prefix == null)
            return null;
        if (prefix.equals(getPrefix()))
            return getNamespaceURI();
        long minOrdinal = 0;
        for (int ns = getNSNodeID(tree.nodeOrdinal[node]); ns >= 0; ns = nextNSNodeID(
            ns, minOrdinal)) {
            int uri = tree.nsNodeUriAtom[ns];
            int pf = tree.nsNodePrefixAtom[ns];
            if (tree.atomString(uri) == null)
                continue;
            if (prefix.equals(tree.atomString(pf)))
                return tree.atomString(uri);
        }
        return null;
    }

    @Override
    public String lookupPrefix(String namespaceURI) {
        if (namespaceURI == null)
            return null;
        if (namespaceURI.equals(getNamespaceURI()))
            return getPrefix();
        long minOrdinal = 0;
        for (int ns = getNSNodeID(tree.nodeOrdinal[node]); ns >= 0; ns = nextNSNodeID(
            ns, minOrdinal)) {
            int uri = tree.nsNodeUriAtom[ns];
            int pf = tree.nsNodePrefixAtom[ns];
            if (tree.atomString(pf) == null)
                continue;
            if (namespaceURI.equals(tree.atomString(uri)))
                return tree.atomString(pf);
        }
        return null;
    }

    /** Unsupported. */
    public void removeAttribute(String name) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    public Attr removeAttributeNode(Attr oldAttr) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    public void removeAttributeNS(String namespaceURI, String localName)
        throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    public void setAttribute(String name, String value) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    public Attr setAttributeNode(Attr newAttr) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    public Attr setAttributeNodeNS(Attr newAttr) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    public void setAttributeNS(String namespaceURI, String qualifiedName,
        String value) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    public void setIdAttribute(String name, boolean isId) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    public void setIdAttributeNode(Attr idAttr, boolean isId)
        throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    public void setIdAttributeNS(String namespaceURI, String localName,
        boolean isId) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }
}
