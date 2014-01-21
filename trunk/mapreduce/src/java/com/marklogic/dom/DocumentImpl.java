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

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Attr;
import org.w3c.dom.CDATASection;
import org.w3c.dom.Comment;
import org.w3c.dom.DOMConfiguration;
import org.w3c.dom.DOMException;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.DocumentType;
import org.w3c.dom.Element;
import org.w3c.dom.EntityReference;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.ProcessingInstruction;
import org.w3c.dom.Text;

import com.marklogic.tree.ExpandedTree;

/**
 * A read-only W3C DOM Node implementation of MarkLogic's internal
 * representation of a document as stored in the expanded tree cache of a forest
 * on disk.
 * 
 * <p>
 * This interface is effectively read-only: Setters and update methods inherited
 * from <code>org.w3c.Node</code> are not supported and will raise an exception
 * if called. To create a modifiable copy of a node, use {@link #cloneNode}.
 * </p>
 * 
 * @author jchen
 */
public class DocumentImpl extends NodeImpl implements Document {
    public static final Log LOG = LogFactory.getLog(DocumentImpl.class);
    private Element documentElement;
    /**
     * owner document for cloneNode
     */
    private Document ownerDocCloned;

    private static DocumentBuilderFactory dbf = null;

    public DocumentImpl(ExpandedTree tree, int node) {
        super(tree, node);
    }

    private static synchronized DocumentBuilderFactory getDocumentBuilderFactory() {
        if (dbf == null) {
            dbf = DocumentBuilderFactory.newInstance();
            dbf.setNamespaceAware(true);
        }
        return dbf;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Namespace declaration is cloned as attribute, whose owner document
     * contains this attribute only.
     * </p>
     */
    public Node cloneNode(boolean deep) {
        try {
            // initialize a new doc owner node
            initClonedOwnerDoc();
        } catch (ParserConfigurationException e) {
            throw new RuntimeException("Internal Error:" + e);
        }
        if (deep) {
            for (NodeImpl n = (NodeImpl) getFirstChild(); n != null; n = (NodeImpl) n
                .getNextSibling()) {
                ownerDocCloned.appendChild(n.cloneNode(ownerDocCloned, true));
            }
        }
        return ownerDocCloned;
    }

    protected void initClonedOwnerDoc() throws ParserConfigurationException {
        ownerDocCloned = getDocumentBuilderFactory().newDocumentBuilder()
            .newDocument();
    }

    @Override
    public String getNodeName() {
        return "#document";
    }

    @Override
    public Document getOwnerDocument() {
        return null;
    }

    protected int getNumChildren() {
        return tree.docNodeNumChildren[tree.nodeRepID[node]];
    }

    public int getFirstChildIndex() {
        return tree.docNodeChildNodeRepID[tree.nodeRepID[node]];
    }

    public NodeList getChildNodes() {
        return new NodeList() {

            public int getLength() {
                return getNumChildren();
            }

            public Node item(int index) {
                return (index < getNumChildren()) ? tree
                    .node(getFirstChildIndex() + index) : null;
            }
        };
    }

    public Node getFirstChild() {
        int i = getFirstChildIndex();
        return (i != Integer.MAX_VALUE) ? tree.node(i) : null;
    }

    public Node getLastChild() {
        int i = tree.docNodeChildNodeRepID[tree.nodeRepID[node]];
        return (i != Integer.MAX_VALUE) ? tree.node(i
            + tree.docNodeNumChildren[node] - 1) : null;
    }

    public boolean hasChildNodes() {
        return (getFirstChildIndex() != Integer.MAX_VALUE);
    }

    protected Node getNextChild(int child) {
        if (child - getFirstChildIndex() + 1 < getNumChildren()) {
            return tree.node(child + 1);
        } else {
            return null;
        }
    }

    protected Node getPreviousChild(int node) {
        if (node != getFirstChildIndex()) {
            return tree.node(node - 1);
        } else {
            return null;
        }
    }

    /** Unsupported. */
    public Node adoptNode(Node arg0) throws DOMException {
    	throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    public Attr createAttribute(String arg0) throws DOMException {
    	throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    public Attr createAttributeNS(String arg0, String arg1)
        throws DOMException {
    	throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    public CDATASection createCDATASection(String arg0) throws DOMException {
    	throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    public Comment createComment(String arg0) {
    	throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    public DocumentFragment createDocumentFragment() {
    	throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    public Element createElement(String arg0) throws DOMException {
    	throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    public Element createElementNS(String arg0, String arg1)
        throws DOMException {
    	throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    public EntityReference createEntityReference(String arg0)
        throws DOMException {
    	throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    public ProcessingInstruction createProcessingInstruction(String arg0,
        String arg1) throws DOMException {
    	throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    public Text createTextNode(String arg0) {
    	throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /**
     * Returns a dummy DocumentTypeImpl object that contains nothing.
     */
    public DocumentType getDoctype() {
        return new DocumentTypeImpl(tree, node);
    }

    public Element getDocumentElement() {
        if (documentElement != null)
            return documentElement;

        NodeList children = getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node n = children.item(i);
            if (n.getNodeType() == Node.ELEMENT_NODE) {
                documentElement = (Element) n;
                break;
            }
        }
        return documentElement;
    }

    @Override
    public boolean isDefaultNamespace(String namespaceURI) {
        Element e = getDocumentElement();
        return e != null ? e.isDefaultNamespace(namespaceURI) : false;
    }

    public String getDocumentURI() {
        return tree.getDocumentURI();
    }

    /** Unsupported. */
    public DOMConfiguration getDomConfig() {
        return null;
    }

    /** Unsupported. */
    public Element getElementById(String arg0) {
        return null;
    }

    public NodeList getElementsByTagNameNS(String namespaceURI, String name) {
        return getElementsByTagNameNSOrNodeName(namespaceURI, name, false);
    }

    public NodeList getElementsByTagName(String localName) {
        return getElementsByTagNameNSOrNodeName(null, localName, true);
    }

    /** Unsupported. */
    public DOMImplementation getImplementation() {
        return null;
    }

    /** Unsupported. */
    public String getInputEncoding() {
        return null;
    }

    /** Unsupported. */
    public boolean getStrictErrorChecking() {
        return false;
    }

    /** Unsupported. */
    public String getXmlEncoding() {
        return null;
    }

    /** Unsupported. */
    public boolean getXmlStandalone() {
        return true;
    }

    public String getXmlVersion() {
        return "1.0";
    }

    /** Unsupported. */
    public Node importNode(Node arg0, boolean arg1) throws DOMException {
        return null;
    }

    @Override
    public String lookupNamespaceURI(String prefix) {
        return getDocumentElement().lookupNamespaceURI(prefix);
    }

    @Override
    public String lookupPrefix(String namespaceURI) {
        return getDocumentElement().lookupPrefix(namespaceURI);
    }

    /** Unsupported. */
    public void normalizeDocument() {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    public Node renameNode(Node arg0, String arg1, String arg2)
        throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    public void setDocumentURI(String arg0) {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    public void setStrictErrorChecking(boolean arg0) {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    public void setXmlStandalone(boolean arg0) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    public void setXmlVersion(String arg0) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

}
