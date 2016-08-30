/*
 * Copyright 2003-2016 MarkLogic Corporation
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
 * representation of an XML or text document as stored in the expanded tree
 * cache of a forest on disk.
 * 
 * <p>
 * This interface is effectively read-only: Setters and update methods inherited
 * from <code>org.w3c.Node</code> are not supported and will raise an exception
 * if called. To create a modifiable copy of a node for XML document, use
 * {@link #cloneNode}. Text document cannot be cloned.
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

    private int isXMLDoc;
    static final int UNKNOWN_TYPE = -1;
    static final int VALID_XML = 0;
    static final int NON_XML = 1;

    public DocumentImpl(ExpandedTree tree, int node) {
        super(tree, node);
        isXMLDoc = UNKNOWN_TYPE;
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
     * Text documents in MarkLogic cannot be cloned.
     * UnsupportedOperationException will be thrown if cloneNode is call on text
     * document. </>
     * <p>
     * DocumentType node will not be cloned as it is not part of the Expanded
     * Tree.</>
     * 
     * 
     */
    @Override
    public Node cloneNode(boolean deep) {
        try {
            if (isXMLDoc == UNKNOWN_TYPE) {
              isXMLDoc = getDocumentType();
            }
            if (isXMLDoc == NON_XML) {
                throw new UnsupportedOperationException(
                    "Text document cannot be cloned");
            }
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
        ownerDocCloned = getDocumentBuilderFactory().newDocumentBuilder().newDocument();
        ownerDocCloned.setDocumentURI(getDocumentURI());
        ownerDocCloned.setXmlVersion(getXmlVersion());
    }
    
    /**
     * Document can be an XML document or a text document.
     * @return true if XML document; otherwise false.
     */
    public boolean isXMLDoc() {
        if (isXMLDoc == UNKNOWN_TYPE) {
          isXMLDoc = getDocumentType();
        }
        return isXMLDoc == VALID_XML;
    }

    /**
     * Check root node of a document to see if it conform to DOM Structure
     * Model. The root node can only be ELEMENT_NODE,
     * PROCESSING_INSTRUCTION_NODE or COMMENT_NODE.
     * 
     * @return 1(NON_XML) if root node violates DOM Structure Model; otherwise
     *         0(VALID_XML).
     */
    private int getDocumentType() {
        NodeList children = getChildNodes();
        int elemCount = 0;
        for (int i = 0; i < children.getLength(); i++) {
            Node n = children.item(i);
            switch (n.getNodeType()) {
            case Node.ELEMENT_NODE:
                elemCount++;
                break;
            case Node.PROCESSING_INSTRUCTION_NODE:
            case Node.COMMENT_NODE:
                continue;
            default:
                return NON_XML;
            }
        }
        return elemCount <= 1 ? VALID_XML : NON_XML;
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

    @Override
    public NodeList getChildNodes() {
        return new NodeList() {

            @Override
            public int getLength() {
                return getNumChildren();
            }

            @Override
            public Node item(int index) {
                return (index < getNumChildren()) ? tree
                    .node(getFirstChildIndex() + index) : null;
            }
        };
    }

    @Override
    public Node getFirstChild() {
        int i = getFirstChildIndex();
        return (i != Integer.MAX_VALUE) ? tree.node(i) : null;
    }

    @Override
    public Node getLastChild() {
        int i = tree.docNodeChildNodeRepID[tree.nodeRepID[node]];
        return (i != Integer.MAX_VALUE) ? tree.node(i
            + tree.docNodeNumChildren[node] - 1) : null;
    }

    @Override
    public boolean hasChildNodes() {
        return (getFirstChildIndex() != Integer.MAX_VALUE);
    }

    @Override
    protected Node getNextChild(int child) {
        if (child - getFirstChildIndex() + 1 < getNumChildren()) {
            return tree.node(child + 1);
        } else {
            return null;
        }
    }

    @Override
    protected Node getPreviousChild(int node) {
        if (node != getFirstChildIndex()) {
            return tree.node(node - 1);
        } else {
            return null;
        }
    }

    /** Unsupported. */
    @Override
    public Node adoptNode(Node arg0) throws DOMException {
    	throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    @Override
    public Attr createAttribute(String arg0) throws DOMException {
    	throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    @Override
    public Attr createAttributeNS(String arg0, String arg1)
        throws DOMException {
    	throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    @Override
    public CDATASection createCDATASection(String arg0) throws DOMException {
    	throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    @Override
    public Comment createComment(String arg0) {
    	throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    @Override
    public DocumentFragment createDocumentFragment() {
    	throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    @Override
    public Element createElement(String arg0) throws DOMException {
    	throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    @Override
    public Element createElementNS(String arg0, String arg1)
        throws DOMException {
    	throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    @Override
    public EntityReference createEntityReference(String arg0)
        throws DOMException {
    	throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    @Override
    public ProcessingInstruction createProcessingInstruction(String arg0,
        String arg1) throws DOMException {
    	throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    @Override
    public Text createTextNode(String arg0) {
    	throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /**
     * Returns a dummy DocumentTypeImpl object that contains nothing. It is not
     * expected to do anything with the returned object.
     */
    @Override
    public DocumentType getDoctype() {
        return new DocumentTypeImpl(tree, node);
    }

    @Override
    public Element getDocumentElement() {
        if (documentElement != null) {
            return documentElement;
        }
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

    @Override
    public String getDocumentURI() {
        return tree.getDocumentURI();
    }

    /** Unsupported. */
    @Override
    public DOMConfiguration getDomConfig() {
        return null;
    }

    /** Unsupported. */
    @Override
    public Element getElementById(String arg0) {
        return null;
    }

    @Override
    public NodeList getElementsByTagNameNS(String namespaceURI, String name) {
        return getElementsByTagNameNSOrNodeName(namespaceURI, name, false);
    }

    @Override
    public NodeList getElementsByTagName(String localName) {
        return getElementsByTagNameNSOrNodeName(null, localName, true);
    }

    /** Unsupported. */
    @Override
    public DOMImplementation getImplementation() {
        return null;
    }

    /** Unsupported. */
    @Override
    public String getInputEncoding() {
        return null;
    }

    /** Unsupported. */
    @Override
    public boolean getStrictErrorChecking() {
        return false;
    }

    /** Unsupported. */
    @Override
    public String getXmlEncoding() {
        return null;
    }

    /** Unsupported. */
    @Override
    public boolean getXmlStandalone() {
        return true;
    }

    @Override
    public String getXmlVersion() {
        return "1.0";
    }

    /** Unsupported. */
    @Override
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
    @Override
    public void normalizeDocument() {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    @Override
    public Node renameNode(Node arg0, String arg1, String arg2)
        throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    @Override
    public void setDocumentURI(String arg0) {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    @Override
    public void setStrictErrorChecking(boolean arg0) {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    @Override
    public void setXmlStandalone(boolean arg0) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    @Override
    public void setXmlVersion(String arg0) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

}
