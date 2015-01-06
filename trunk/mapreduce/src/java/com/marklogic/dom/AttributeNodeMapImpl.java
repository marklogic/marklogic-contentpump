/*
 * Copyright 2003-2015 MarkLogic Corporation
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
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import com.marklogic.tree.ExpandedTree;

public class AttributeNodeMapImpl implements NamedNodeMap {
    public static final Log LOG = LogFactory
        .getLog(AttributeNodeMapImpl.class);
    protected ElementImpl element;
    protected Attr[] nsDecl;
    private static DocumentBuilderFactory dbf = null;

    public AttributeNodeMapImpl(ElementImpl element) {
        this.element = element;
    }

    public int getLength() {
        if (LOG.isTraceEnabled()) {
            LOG.trace(element.getNodeName() + "@NumAttr:" + getNumAttr()
                + " NumNSDecl:" + element.getNumNSDecl());
        }
        return getNumAttr() + element.getNumNSDecl();
    }

    /*
     * Exclude namespace declaration
     */
    protected int getNumAttr() {
        int num = element.tree.elemNodeNumAttributes[element.tree.nodeRepID[element.node]];
        return num >= 0 ? num : 0;
    }

    public Node getNamedItem(String name) {
        if (LOG.isTraceEnabled()) {
            LOG.trace(this.getClass().getSimpleName() + ".getNamedItem("
                + element.node + ", " + name + ")");
        }
        if (name == null)
            return null;
        for (int i = 0; i < getLength(); i++)
            if (name.equals(item(i).getNodeName()))
                return item(i);
        return null;
    }

    public Node getNamedItemNS(String namespaceURI, String localName)
        throws DOMException {
        if (LOG.isTraceEnabled()) {
            LOG.trace(this.getClass().getSimpleName() + ".getNamedItemNS("
                + element.node + ", " + namespaceURI + ", " + localName + ")");
        }
        if (localName == null)
            return null;
        for (int i = 0; i < getLength(); i++) {
            if ((namespaceURI == null) != (item(i).getNamespaceURI() == null))
                continue;
            if (namespaceURI != null
                && !namespaceURI.equals(item(i).getNamespaceURI()))
                continue;
            if (localName.equals(item(i).getLocalName()))
                return item(i);
        }
        return null;
    }

    private static synchronized DocumentBuilderFactory getDocumentBuilderFactory() {
        if (dbf == null) {
            dbf = DocumentBuilderFactory.newInstance();
            dbf.setNamespaceAware(true);
        }
        return dbf;
    }

    protected Document getClonedOwnerDoc() throws ParserConfigurationException {
        return getDocumentBuilderFactory().newDocumentBuilder().newDocument();
    }

    /**
     * Returns the indexth item in the map. If index is greater than or equal to
     * the number of nodes in this map, this returns null; if the item returned
     * is a namespace declaration, it is represented as an attribute whose owner
     * document is a document containing the attribute only.
     */
    public Node item(int index) {
        try {
            return item(index, null);
        } catch (ParserConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    protected Node item(int index, Document ownerDoc)
        throws ParserConfigurationException {
        int numAttr = getNumAttr();
        ExpandedTree tree = element.tree;
        if (index < numAttr) {
            if (LOG.isTraceEnabled()) {
                LOG.trace(this.getClass().getSimpleName() + ".item("
                    + element.node + ", " + index + ")");
            }
            return tree
                .node(tree.elemNodeAttrNodeRepID[tree.nodeRepID[element.node]]
                    + index);
        } else {
            int nsIdx = index - numAttr;
            // if nsDecl is initialized, return it
            if (nsDecl != null)
                return nsDecl[nsIdx];

            // create owner doc
            if (ownerDoc == null) {
                ownerDoc = getClonedOwnerDoc();
            }

            nsDecl = new Attr[element.getNumNSDecl()];
            // ordinal of the element node
            long minimal = tree.nodeOrdinal[element.node];
            int count = 0;
            for (int ns = element.getNSNodeID(minimal, minimal); ns >= 0
                && count < element.getNumNSDecl(); ns = element.nextNSNodeID(
                ns, minimal)) {
                String uri = tree.atomString(tree.nsNodeUriAtom[ns]);
                String prefix = tree.atomString(tree.nsNodePrefixAtom[ns]);
                Attr attr = null;
                String name = null;
                try {
                    if (prefix != null && "".equals(prefix) == false) {
                        name = "xmlns:" + prefix;
                    } else {
                        name = "xmlns";
                    }
                    attr = ownerDoc.createAttributeNS(
                        "http://www.w3.org/2000/xmlns/", name);
                    attr.setValue(uri);
                } catch (DOMException e) {
                    throw new RuntimeException(e);
                }
                nsDecl[count] = attr;
                count++;
            }

            if (nsDecl != null && nsIdx < count) {
                return nsDecl[nsIdx];
            }
            return null;
        }
    }

    /** Unsupported. */
    public Node removeNamedItem(String name) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    public Node removeNamedItemNS(String namespaceURI, String localName)
        throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    public Node setNamedItem(Node arg) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    public Node setNamedItemNS(Node arg) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }
}
