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

import org.w3c.dom.Attr;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.TypeInfo;

import com.marklogic.tree.ExpandedTree;

/**
 * A read-only W3C DOM Node implementation of MarkLogic's
 * internal representation of a document element as stored in the expanded 
 * tree cache of a forest on disk. 
 * 
 * <p>
 * This interface is effectively read-only: Setters and update methods 
 * inherited from <code>org.w3c.Node</code> are not supported and will raise
 * an exception if called. To create a modifiable copy of a node, use
 * {@link #cloneNode}.
 * </p>
 * 
 * @author jchen
 */
public class ElementImpl extends NodeImpl implements Element {

    protected AttributeNodeMapImpl attributes;
    protected int numNSDecl;
    public ElementImpl(ExpandedTree tree, int node) {
        super(tree, node);
        attributes = new AttributeNodeMapImpl(this);
        numNSDecl = -1;
    }
	
    public Node cloneNode(boolean deep) {
        throw new UnsupportedOperationException();
    }
    
	public Node cloneNode(Document doc, boolean deep) {
        Element elem = doc.createElementNS(getNamespaceURI(), getTagName());
        elem.setPrefix(getPrefix());

        for (int i = 0; i < attributes.getLength(); i++) {
            Attr attr = (Attr) attributes.item(i);
            if(attr instanceof AttrImpl) {
                elem.setAttributeNode((Attr)((AttrImpl)attr).cloneNode(doc, deep));
            } else {
                //ns decl, stored as Java DOM Attr
                Attr clonedAttr = doc.createAttribute(attr.getName());
                clonedAttr.setValue(attr.getValue());
                elem.setAttributeNode(clonedAttr);
            }
        }
        
        if(deep) {
            //clone children 
            NodeList list = getChildNodes();
            for(int i=0; i<list.getLength(); i++) {
                NodeImpl n = (NodeImpl)list.item(i);
                Node c = n.cloneNode(doc, true);
                elem.appendChild(c);
            }
        }
        return elem;
	}
	
	public String getAttribute(String name) {
	    return getAttributeNode(name).getValue();
	}

	public Attr getAttributeNode(String name) {
	    		return (AttrImpl)attributes.getNamedItem(name);
	}

	// TODO
	public Attr getAttributeNodeNS(String namespaceURI, String localName)
			throws DOMException {
		assert (false);
		return null;
	}

	// TODO
	public String getAttributeNS(String namespaceURI, String localName)
			throws DOMException {
		assert (false);
		return null;
	}

	@Override
	public NamedNodeMap getAttributes() {
		return attributes;
	}

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
	
	public NodeList getElementsByTagNameNS(String namespaceURI, String name) {
		return getElementsByTagNameNSOrNodeName(namespaceURI,name,false);
	}

	public NodeList getElementsByTagName(String localName) {
		return getElementsByTagNameNSOrNodeName(null,localName,true);
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
		return ns == null || ns.equals("") ? getTagName() : ns + ":" + getTagName();
	}
	
	protected int getPrefixID(int uriAtom) {
		int a = -1;
		boolean useDefaultNS = true;
		ArrayList<Integer> ubp = new ArrayList<Integer>();
		long minOrdinal = 0;
		for ( int ns = getNSNodeID(tree.nodeOrdinal[node]); ns >= 0 ; ns = nextNSNodeID(ns,minOrdinal) ) {
    		int uri = tree.nsNodeUriAtom[ns];
    		int prefix = tree.nsNodePrefixAtom[ns];
    		if (tree.atomString(uri) == null) { ubp.add(prefix); continue; }
    		if (uri != uriAtom) { useDefaultNS &= (tree.atomString(prefix) != null); continue; }
    		if (ubp.contains(prefix)) continue;
    		if (tree.atomString(prefix) != null) {if (a == -1) a = prefix; continue;}
    		if (useDefaultNS) return prefix; 
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
		if (tree.atomString(ns) != null)  ns = getPrefixID(ns);
	    String r = (ns != -1) ? tree.atomString(ns) : null;
	    return r;
	}

	@Override
	protected Node getPreviousChild(int child) {
		return (child != tree.elemNodeChildNodeRepID[tree.nodeRepID[node]]) ? 
				tree.node(child - 1) : null;
	}

	public TypeInfo getSchemaTypeInfo() {
		return null;
	}

	public String getTagName() {
		return tree
				.atomString(tree.nodeNameNameAtom[tree.elemNodeNodeNameRepID[tree.nodeRepID[node]]]);
	}

	// TODO
	public boolean hasAttribute(String name) {
		assert (false);
		return false;
	}

	// TODO
	public boolean hasAttributeNS(String namespaceURI, String localName)
			throws DOMException {
		assert (false);
		return false;
	}

	public boolean hasAttributes() {
	    return getAttributes().getLength() > 0;
	}

	@Override
	public boolean hasChildNodes() {
		return (tree.elemNodeChildNodeRepID[tree.nodeRepID[node]] != Integer.MAX_VALUE);
	}

	// The following page gives algorithm for the following functions
	// http://www.w3.org/TR/DOM-Level-3-Core/namespaces-algorithms.html#lookupNamespacePrefixAlgo
	
	@Override
    public String lookupNamespaceURI(String prefix) {
        if (prefix == null) return null;
        if (prefix.equals(getPrefix())) return getNamespaceURI();
		int a = -1;
		boolean useDefaultNS = true;
		ArrayList<Integer> ubp = new ArrayList<Integer>();
		long minOrdinal = 0;
		for ( int ns = getNSNodeID(tree.nodeOrdinal[node]); ns >= 0 ; ns = nextNSNodeID(ns,minOrdinal) ) {
    		int uri = tree.nsNodeUriAtom[ns];
    		int pf = tree.nsNodePrefixAtom[ns];
    		if (tree.atomString(uri) == null) { ubp.add(pf); continue; }
    		if (tree.atomString(pf) != prefix) { useDefaultNS &= (tree.atomString(pf) != null); continue; }
    		if (ubp.contains(pf)) continue;
    		if (tree.atomString(pf) != null) {if (a == -1) a = pf; continue;}
    		if (useDefaultNS) return null; 
    	} 
    	return null;
    }

	@Override
	public String lookupPrefix(String namespaceURI) {
        if (namespaceURI == null) return null;
        if (namespaceURI.equals(getNamespaceURI())) return getPrefix();
		int a = -1;
		boolean useDefaultNS = true;
		ArrayList<Integer> ubp = new ArrayList<Integer>();
		long minOrdinal = 0;
		for ( int ns = getNSNodeID(tree.nodeOrdinal[node]); ns >= 0 ; ns = nextNSNodeID(ns,minOrdinal) ) {
    		int uri = tree.nsNodeUriAtom[ns];
    		int prefix = tree.nsNodePrefixAtom[ns];
    		if (tree.atomString(uri) == null) { ubp.add(prefix); continue; }
    		if (tree.atomString(uri) != namespaceURI) { useDefaultNS &= (tree.atomString(prefix) != null); continue; }
    		if (ubp.contains(prefix)) continue;
    		if (tree.atomString(prefix) != null) {if (a == -1) a = prefix; continue;}
    		if (useDefaultNS) return null; 
    	} 
    	return null;
	}
	
	// TODO investigate function meaning
	protected String lookupPrefix(String namespaceURI,Node root) {
		int a = -1;
		boolean useDefaultNS = true;
		ArrayList<Integer> ubp = new ArrayList<Integer>();
		
		/* Logic from NSNodeIterator(const Node& node, const Node& root) */
		long minOrdinal = (this == root?0:tree.nodeOrdinal[tree.nodeRepID[node]]);
		if (tree.nodeOrdinal[tree.nodeRepID[node]] < minOrdinal) return null;
		// TODO check what parentNSNodeRepID logic is all about
		/* END */
		
		for ( int ns = getNSNodeID(tree.nodeOrdinal[tree.nodeRepID[node]]); ns > 0 ; ns = nextNSNodeID(ns,minOrdinal) ) {
			int uri = tree.nsNodeUriAtom[ns];
			int prefix = tree.nsNodePrefixAtom[ns];
			if (tree.atomString(uri) == null) { ubp.add(prefix); continue; }
			if (!namespaceURI.equals(tree.atomString(uri))) { useDefaultNS &= (tree.atomString(prefix) != null); continue; }
			if (ubp.contains(prefix)) continue;
			if (tree.atomString(prefix) != null) {if (a < 0) a = prefix; continue;}
			if (useDefaultNS) return (prefix >= 0) ? tree.atomString(prefix) : null;
		} 
		return (a >= 0) ? tree.atomString(a) : null;
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
