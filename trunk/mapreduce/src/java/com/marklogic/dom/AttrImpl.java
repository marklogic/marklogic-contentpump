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
import org.w3c.dom.Node;
import org.w3c.dom.TypeInfo;

import com.marklogic.tree.ExpandedTree;

/**
 * A read-only W3C DOM Node implementation of MarkLogic's internal
 * representation of an element attribute as stored in the expanded tree cache
 * of a forest on disk.
 * 
 * <p>
 * This class is effectively read-only: Setters and update methods inherited
 * from <code>org.w3c.Node</code> are not supported and will raise an exception
 * if called.
 * </p>
 * 
 * @author jchen
 */
public class AttrImpl extends NodeImpl implements Attr {
    public static final Log LOG = LogFactory.getLog(AttrImpl.class);

    public AttrImpl(ExpandedTree tree, int node) {
        super(tree, node);
    }

    protected Node cloneNode(Document doc, boolean deep) {
        Attr attr = doc.createAttributeNS(getNamespaceURI(), getLocalName());
        attr.setValue(getValue());
        attr.setPrefix(getPrefix());
        return attr;
    }

    protected int getNodeID() {
        return tree.attrNodeNodeNameRepID[tree.nodeRepID[node]];
    }

    /** {@inheritDoc} */
    @Override
    public String getLocalName() {
        return tree.atomString(tree.nodeNameNameAtom[getNodeID()]);
    }

    /** {@inheritDoc} */
    public String getName() {
        String prefix = getPrefix();
        return prefix == null || prefix.equals("") ? getLocalName() : prefix
            + ":" + getLocalName();
    }

    /** {@inheritDoc} */
    @Override
    public String getNamespaceURI() {
        return tree.atomString(tree.nodeNameNamespaceAtom[getNodeID()]);
    }

    /** {@inheritDoc} */
    @Override
    public String getNodeName() {
        return getName();
    }

    /** {@inheritDoc} */
    @Override
    public Node getNextSibling() {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Node getParentNode() {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Node getPreviousSibling() {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public String getNodeValue() {
        return getValue();
    }
    /** 
     * {@inheritDoc}
     * OwnerElement for a namespace attribute is null.
     */
    public Element getOwnerElement() {
        return (Element) tree.node(tree.nodeParentNodeRepID[node]);
    }

    @Override
    protected int getPrefixID(int uriAtom) {
        int parentNodeRepID = tree.nodeParentNodeRepID[node];
        if (parentNodeRepID == -1)
            parentNodeRepID = node;
        ArrayList<Integer> ubp = new ArrayList<Integer>();
        long sum_ordinal = tree.ordinal + tree.nodeOrdinal[parentNodeRepID];
        for (int ns = getNSNodeID(sum_ordinal); ns >= 0; ns = nextNSNodeID(ns,
            0)) {
            int uri = tree.nsNodeUriAtom[ns];
            int prefix = tree.nsNodePrefixAtom[ns];
            if (tree.atomString(uri) == null) {
                ubp.add(prefix);
                continue;
            }
            if (uri != uriAtom)
                continue;
            if (ubp.contains(prefix))
                continue;
            if (tree.atomString(prefix) == null)
                continue;
            return prefix;
        }
        return -1;
    }

    /** {@inheritDoc} */
    @Override
    public String getPrefix() {
        int ns = tree.nodeNameNamespaceAtom[tree.attrNodeNodeNameRepID[tree.nodeRepID[node]]];
        if (ns < 0)
            return null;
        String preserved = builtinNSPrefix(getNamespaceURI());
        if (preserved != null)
            return preserved;
        if (tree.atomString(ns) != null)
            ns = getPrefixID(ns);
        return (ns >= 0) ? tree.atomString(ns) : null;
    }

    /** Unsupported. */
    public TypeInfo getSchemaTypeInfo() {
        return null;
    }

    /** {@inheritDoc} */
    public boolean getSpecified() {
        return true;
    }

    /** {@inheritDoc} */
    public String getValue() {
        return tree.getText(tree.attrNodeTextRepID[tree.nodeRepID[node]]);
    }

    /** Unsupported. */
    public boolean isId() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public String lookupNamespaceURI(String prefix) {
        return getOwnerElement().lookupNamespaceURI(prefix);
    }

    /** {@inheritDoc} */
    @Override
    public String lookupPrefix(String namespaceURI) {
        return getOwnerElement().lookupPrefix(namespaceURI);
    }

    /** Unsupported. */
    public void setValue(String value) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }
}
