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

import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Attr;
import org.w3c.dom.DOMException;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import com.marklogic.tree.ExpandedTree;

public class AttributeNodeMapImpl implements NamedNodeMap {
	ElementImpl element;
	
	public AttributeNodeMapImpl(ElementImpl element) {
		this.element = element;
	}
	
	public int getLength() {
		return getNumAttr() + element.getNumNSDecl();
	}
	
	/*
	 * Exclude namespace declaration
	 */
	protected int getNumAttr() {
	    int num = element.tree.elemNodeNumAttributes[element.tree.nodeRepID[element.node]];
	    return num != Integer.MAX_VALUE? num : 0;   
	}

	public Node getNamedItem(String name) {
        if (NodeImpl.trace)
            System.out.println(this.getClass().getSimpleName()
                + ".getNamedItem(" + element.node + ", " + name + ")");
		if (name == null) return null;
    	for (int i = 0; i < getLength(); i++)
			if (name.equals(item(i).getNodeName())) return item(i);
		return null;
	}

	public Node getNamedItemNS(String namespaceURI, String localName)
			throws DOMException {
        if (NodeImpl.trace)
            System.out.println(this.getClass().getSimpleName()
                + ".getNamedItemNS(" + element.node + ", " + namespaceURI
                + ", " + localName + ")");
    	if (localName == null) return null;
    	for (int i = 0; i < getLength(); i++) {
    		if ((namespaceURI == null) != (item(i).getNamespaceURI() == null)) continue;
    		if (namespaceURI != null && !namespaceURI.equals(item(i).getNamespaceURI())) continue;    			
			if (localName.equals(item(i).getLocalName())) return item(i);
    	}
		return null;
	}

	public Node item(int index) {
        int numAttr = getNumAttr();
        ExpandedTree tree = element.tree;
        if (index < numAttr) {
            if (NodeImpl.trace)
                System.out.println(this.getClass().getSimpleName() + ".item("
                    + element.node + ", " + index + ")");
            return tree
                .node(tree.elemNodeAttrNodeRepID[tree.nodeRepID[element.node]]
                    + index);
        } else {
            int nsIdx = index - numAttr;
            int count = 0;
            long minimal = tree.nodeOrdinal[element.node];
            for (int ns = element.getNSNodeID(minimal, minimal); ns >= 0; ns = element
                .nextNSNodeID(ns, minimal)) {
                if (count == nsIdx) {
                    String uri = tree.atomString(tree.nsNodeUriAtom[ns]);
                    String prefix = tree.atomString(tree.nsNodePrefixAtom[ns]);
                    Attr attr = null;
                    try {
                        if (prefix != null && "".equals(prefix) == false) {
                            attr = tree.getClonedDocOwner().createAttribute(
                                "xmlns:" + prefix);
                        } else {
                            attr = tree.getClonedDocOwner().createAttribute(
                                "xmlns");
                        }
                        attr.setNodeValue(uri);
                    } catch (DOMException e) {
                        throw new RuntimeException(e);
                    } catch (ParserConfigurationException e) {
                        throw new RuntimeException(e);
                    }
                    return attr;
                }
                count++;
            }
            return null;
        }
	}

	public Node removeNamedItem(String name) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}

	public Node removeNamedItemNS(String namespaceURI, String localName)
			throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}

	public Node setNamedItem(Node arg) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}

	public Node setNamedItemNS(Node arg) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}
}
