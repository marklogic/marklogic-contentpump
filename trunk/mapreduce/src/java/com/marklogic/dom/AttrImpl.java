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

    @Override
    public String getLocalName() {
        return tree.atomString(tree.nodeNameNameAtom[tree.attrNodeNodeNameRepID[tree.nodeRepID[node]]]); 
    }

    @Override
    public String getName() {
    	return tree.atomString(tree.nodeNameNameAtom[tree.attrNodeNodeNameRepID[tree.nodeRepID[node]]]); 
    }

    @Override
    public String getNamespaceURI() {
        return tree.atomString(tree.nodeNameNamespaceAtom[tree.attrNodeNodeNameRepID[tree.nodeRepID[node]]]);
    }

    @Override
    public Node getNextSibling() {
    	return null;
    }
    
    @Override
    public String getNodeName() {
    	return getName(); 
    }
    
    @Override
    public String getNodeValue() {
    	return getValue(); 
    }
    
    @Override
    public Element getOwnerElement() {
        return (Element)tree.node(tree.nodeParentNodeRepID[node]);
    }

    @Override
    public Node getParentNode() {
    	return null;
    }
    
    // TODO
    @Override
    public String getPrefix() {
    	return "TODO";
    }
    
    @Override
    public Node getPreviousSibling() {
    	return null;
    }

    @Override
    public TypeInfo getSchemaTypeInfo() {
        return null;
    }

    @Override
    public boolean getSpecified() {
        return true;
    }

    @Override
    public String getValue() {
    	return tree.getText(tree.attrNodeTextRepID[tree.nodeRepID[node]]);
    }

    @Override
    public boolean isId() {
        return false;
    }

    @Override
    public void setValue(String value) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }
}
