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

import org.w3c.dom.DOMException;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

public class AttributeNodeMapImpl implements NamedNodeMap {

	ElementImpl element;
	
	public AttributeNodeMapImpl(ElementImpl element) {
		this.element = element;
	}
	
	@Override
	public int getLength() {
		return element.tree.elemNodeNumAttributes[element.tree.nodeRepID[element.node]];
	}

	@Override
	public Node getNamedItem(String name) {
    	if (NodeImpl.trace) System.out.println(this.getClass().getSimpleName() + ".getNamedItem(" + element.node + ", " + name + ")");
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Node getNamedItemNS(String namespaceURI, String localName)
			throws DOMException {
    	if (NodeImpl.trace) System.out.println(this.getClass().getSimpleName() + ".getNamedItemNS(" + element.node + ", " + namespaceURI + ", " + localName + ")");
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Node item(int index) {
    	if (NodeImpl.trace) System.out.println(this.getClass().getSimpleName() + ".item(" + element.node + ", " + index + ")");
		return element.tree.node(element.tree.elemNodeAttrNodeRepID[element.tree.nodeRepID[element.node]]+index);
	}

	@Override
	public Node removeNamedItem(String name) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}

	@Override
	public Node removeNamedItemNS(String namespaceURI, String localName)
			throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}

	@Override
	public Node setNamedItem(Node arg) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}

	@Override
	public Node setNamedItemNS(Node arg) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}
}
