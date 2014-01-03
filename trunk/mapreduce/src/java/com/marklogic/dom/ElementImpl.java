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

import org.w3c.dom.Attr;
import org.w3c.dom.DOMException;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.TypeInfo;
import java.util.ArrayList;

import com.marklogic.tree.ExpandedTree;

public class ElementImpl extends NodeImpl implements Element {

	public ElementImpl(ExpandedTree tree, int node) {
		super(tree, node);
	}

	// TODO
	public String getAttribute(String name) {
	    return getAttributeNode(name).getValue();
	}

	// TODO
	public Attr getAttributeNode(String name) {
	    NamedNodeMap nnMap = getAttributes();
		return (AttrImpl)nnMap.getNamedItem(name);
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
		return new AttributeNodeMapImpl(this);
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
		return ns == null ? getTagName() : ns + ":" + getTagName();
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
		return (tree.elemNodeAttrNodeRepID[tree.nodeRepID[node]] != Integer.MAX_VALUE);
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
        return null;
    }

	@Override
	public String lookupPrefix(String namespaceURI) {
        if (namespaceURI == null) return null;
        if (namespaceURI.equals(getNamespaceURI())) return getPrefix();
        for (Node parent = this.getParentNode(); parent != null; parent = parent.getParentNode()) {
          if (parent.getNodeType() != Node.ELEMENT_NODE) 
        	  continue;
          String found = ((ElementImpl)parent).lookupPrefix(namespaceURI,this);
          if (found != null) return found;
        } 
        return null;
	}
	
	protected String lookupPrefix(String namespaceURI,Node root) {
		int a = Integer.MAX_VALUE;
		boolean useDefaultNS = true;
		ArrayList<Integer> ubp = new ArrayList<Integer>();
		
		/* Logic from NSNodeIterator(const Node& node, const Node& root) */
		long minOrdinal = (this == root?0:tree.nodeOrdinal[tree.nodeRepID[node]]);
		if (tree.nodeOrdinal[tree.nodeRepID[node]] < minOrdinal) return null;
		// TODO check what parentNSNodeRepID logic is all about
		/* END */
		
		for ( int ns = getNSNodeID(tree.nodeOrdinal[tree.nodeRepID[node]]); ns != Integer.MAX_VALUE ; ns = nextNSNodeID(ns,minOrdinal) ) {
			int uri = tree.nsNodeUriAtom[ns];
			int prefix = tree.nsNodePrefixAtom[ns];
			if (tree.atomString(uri) == null) { ubp.add(prefix); continue; }
			if (!namespaceURI.equals(tree.atomString(uri))) { useDefaultNS &= (tree.atomString(prefix) != null); continue; }
			if (ubp.contains(prefix)) continue;
			if (tree.atomString(prefix) != null) {if (a == Integer.MAX_VALUE) a = prefix; continue;}
			if (useDefaultNS) return (prefix != Integer.MAX_VALUE) ? tree.atomString(prefix) : null;
		} 
		return (a != Integer.MAX_VALUE) ? tree.atomString(a) : null;
	}
	
	public void removeAttribute(String name) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}

	public Attr removeAttributeNode(Attr oldAttr) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}

	public void removeAttributeNS(String namespaceURI, String localName)
			throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}

	public void setAttribute(String name, String value) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}

	public Attr setAttributeNode(Attr newAttr) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}

	public Attr setAttributeNodeNS(Attr newAttr) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}

	public void setAttributeNS(String namespaceURI, String qualifiedName,
			String value) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}

	public void setIdAttribute(String name, boolean isId) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}

	public void setIdAttributeNode(Attr idAttr, boolean isId)
			throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}

	public void setIdAttributeNS(String namespaceURI, String localName,
			boolean isId) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}
}
