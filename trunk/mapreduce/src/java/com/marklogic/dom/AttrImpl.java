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
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.TypeInfo;

import com.marklogic.tree.ExpandedTree;

public class AttrImpl extends NodeImpl implements Attr {

    public AttrImpl(ExpandedTree tree, int node) {
        super(tree, node);
    }
    
    protected int getNodeID() {
    	return tree.attrNodeNodeNameRepID[tree.nodeRepID[node]];
    }
    
    @Override
    public String getLocalName() {
        return tree.atomString(tree.nodeNameNameAtom[getNodeID()]); 
    }

    public String getName() {
    	return tree.atomString(tree.nodeNameNameAtom[getNodeID()]); 
    }

    @Override
    public String getNamespaceURI() {
        return tree.atomString(tree.nodeNameNamespaceAtom[getNodeID()]);
    }

    @Override
    public String getNodeName() {
    	return getName(); 
    }
    
    @Override
    public String getNodeValue() {
    	return getValue(); 
    }
    
    public Element getOwnerElement() {
        return (Element)tree.node(tree.nodeParentNodeRepID[node]);
    }

    @Override
    protected int getPrefixID(int uriAtom) {
		int parentNodeRepID = tree.nodeParentNodeRepID[node];
		if (parentNodeRepID == Integer.MAX_VALUE) parentNodeRepID = node;
		ArrayList<Integer> ubp = new ArrayList<Integer>();
		long sum_ordinal = tree.ordinal+tree.nodeOrdinal[parentNodeRepID];
    	for ( int ns = getNSNodeID(sum_ordinal); ns != Integer.MAX_VALUE ; ns = nextNSNodeID(ns,0) ) {
    		int uri = tree.nsNodeUriAtom[ns];
    		int prefix = tree.nsNodePrefixAtom[ns];
    		if (tree.atomString(uri) == null) { ubp.add(prefix); continue; }
    		if (uri != uriAtom)  continue; 
    		if (ubp.contains(prefix)) continue;
    		if (tree.atomString(prefix) != null)  continue;
    		return prefix; 
    	} 
    	return Integer.MAX_VALUE;
	}
    
	@Override
	public String getPrefix() {
		int ns = tree.nodeNameNamespaceAtom[tree.elemNodeNodeNameRepID[tree.nodeRepID[node]]];
		if (ns < 0) return null;
		if (tree.atomString(ns) != null)  ns = getPrefixID(ns);
	    return (ns != Integer.MAX_VALUE) ? tree.atomString(ns) : null;
	}
	
    public TypeInfo getSchemaTypeInfo() {
        return null;
    }

    public boolean getSpecified() {
        return true;
    }

    public String getValue() {
    	return tree.getText(tree.attrNodeTextRepID[tree.nodeRepID[node]]);
    }

    public boolean isId() {
        return false;
    }
    
	@Override
    public String lookupNamespaceURI(String prefix) {
		return getOwnerElement().lookupNamespaceURI(prefix);
    }

	@Override
	public String lookupPrefix(String namespaceURI) {
		return getOwnerElement().lookupPrefix(namespaceURI);
	}

    public void setValue(String value) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }
}
