package com.marklogic.dom;

import org.w3c.dom.Attr;
import org.w3c.dom.DOMException;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.TypeInfo;

import com.marklogic.tree.ExpandedTree;

public class ElementImpl extends NodeImpl implements Element {

	public ElementImpl(ExpandedTree tree, int node) {
		super(tree, node);
	}

	// TODO
	@Override
	public String getAttribute(String name) {
		assert (false);
		return null;
	}

	// TODO
	@Override
	public Attr getAttributeNode(String name) {
		assert (false);
		return null;
	}

	// TODO
	@Override
	public Attr getAttributeNodeNS(String namespaceURI, String localName)
			throws DOMException {
		assert (false);
		return null;
	}

	// TODO
	@Override
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
			@Override
			public int getLength() {
				return tree.elemNodeNumChildren[tree.nodeRepID[node]];
			}

			@Override
			public Node item(int index) {
				return (index < getLength()) ? tree
						.node(tree.elemNodeChildNodeRepID[tree.nodeRepID[node]]
								+ index) : null;
			}
		};
	}

	// TODO
	@Override
	public NodeList getElementsByTagName(String name) {
		assert (false);
		return null;
	}

	// TODO
	@Override
	public NodeList getElementsByTagNameNS(String namespaceURI, String localName)
			throws DOMException {
		assert (false);
		return null;
	}

	protected int getNumChildren() {
		return tree.elemNodeNumChildren[tree.nodeRepID[node]];
	}

	protected int getFirstChildIndex() {
		return tree.elemNodeChildNodeRepID[tree.nodeRepID[node]];
	}

	@Override
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
		return getTagName();
	}

    protected int getNSNodeID() {
        int R = tree.numNSNodeReps;
        if (R == 0) return -1;
        int L = 0;
        long ordinal = tree.nodeOrdinal[node];
        while (L + 1 < R) {
            int M = (L + R) >>> 1;
            if (ordinal < tree.nsNodeOrdinal[M])
                R = M;
            else
                L = M;
        }
        if (ordinal < tree.nsNodeOrdinal[L])
            --L;
        for (;;) {
            if (L < 0)
                break;
            if (tree.nsNodePrefixAtom[L] != Integer.MAX_VALUE)
                break;
            L = tree.nsNodePrevNSNodeRepID[L];
        }
        return L;
    }
	
	@Override
	public String getPrefix() {
	    int ns = getNSNodeID();
	    return (ns >= 0) ? tree.atomString(tree.nsNodePrefixAtom[ns]) : null;
	}

	// TODO
	@Override
	protected Node getPreviousChild(int child) {
		assert (false);
		return null;
	}

	@Override
	public TypeInfo getSchemaTypeInfo() {
		return null;
	}

	@Override
	public String getTagName() {
		return tree
				.atomString(tree.nodeNameNameAtom[tree.elemNodeNodeNameRepID[tree.nodeRepID[node]]]);
	}

	// TODO
	@Override
	public boolean hasAttribute(String name) {
		assert (false);
		return false;
	}

	// TODO
	@Override
	public boolean hasAttributeNS(String namespaceURI, String localName)
			throws DOMException {
		assert (false);
		return false;
	}

	@Override
	public boolean hasAttributes() {
		return (tree.elemNodeAttrNodeRepID[tree.nodeRepID[node]] != Integer.MAX_VALUE);
	}

	@Override
	public boolean hasChildNodes() {
		return (tree.elemNodeChildNodeRepID[tree.nodeRepID[node]] != Integer.MAX_VALUE);
	}

	@Override
	public void removeAttribute(String name) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}

	@Override
	public Attr removeAttributeNode(Attr oldAttr) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}

	@Override
	public void removeAttributeNS(String namespaceURI, String localName)
			throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}

	@Override
	public void setAttribute(String name, String value) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}

	@Override
	public Attr setAttributeNode(Attr newAttr) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}

	@Override
	public Attr setAttributeNodeNS(Attr newAttr) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}

	@Override
	public void setAttributeNS(String namespaceURI, String qualifiedName,
			String value) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}

	@Override
	public void setIdAttribute(String name, boolean isId) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}

	@Override
	public void setIdAttributeNode(Attr idAttr, boolean isId)
			throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}

	@Override
	public void setIdAttributeNS(String namespaceURI, String localName,
			boolean isId) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}
}
