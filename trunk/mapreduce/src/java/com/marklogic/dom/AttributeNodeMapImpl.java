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
