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
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.UserDataHandler;

import com.marklogic.tree.ExpandedTree;
import com.marklogic.tree.NodeKind;

/**
 * A read-only W3C DOM Node implementation of MarkLogic's
 * internal representation of a node as stored in the expanded 
 * tree cache of a forest on disk. 
 * 
 * <p>
 * This interface is effectively read-only: Setters and update methods 
 * inherited from <code>org.w3c.Node</code> are not supported and will raise
 * an exception if called.
 * </p>
 * 
 * @author jchen
 */
public abstract class NodeImpl implements Node {
    public static final Log LOG = LogFactory.getLog(NodeImpl.class);
    
    private static final NodeList emptyNodeList = new NodeList() {
    
        public int getLength() {
            return 0;
        }
    
        public Node item(int index) {
            return null;
        }
    };

    protected final ExpandedTree tree;

    protected final int node;

    /**
     * No public constructor; only subclasses of Node should be instantiated
     */
    NodeImpl(ExpandedTree tree, int node) {
        this.tree = tree;
        this.node = node;
    }

    public ExpandedTree getExpandedTree() {
        return tree;
    }

    /** Unsupported. */
    public Node appendChild(Node newChild) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /**
     * CloneNode is only supported for document node. 
     * <p>
     * {@inheritDoc}
     * </p>
     */
    public Node cloneNode(boolean deep) {
        throw new UnsupportedOperationException();
    }
    
    /**
     * Given a owner document, clone the node
     * @param doc owner document
     * @param deep the flag to indicate deep clone or not
     * @return
     */
    public Node cloneNode(Document doc, boolean deep) {
        return null;
    }
    
    public short compareDocumentPosition(Node other) throws DOMException {
        if (other instanceof NodeImpl) {
            NodeImpl otherNode = (NodeImpl)other;
            if (this.tree == otherNode.tree) {
                if (tree.nodeOrdinal[node] > tree.nodeOrdinal[otherNode.node]) {
                	int ancestor = tree.nodeParentNodeRepID[node];
                    while (ancestor!= Integer.MAX_VALUE && 
                    	   tree.nodeOrdinal[ancestor] >= tree.nodeOrdinal[otherNode.node]) {
                    	if (ancestor == otherNode.node) 
                    		return DOCUMENT_POSITION_CONTAINS | DOCUMENT_POSITION_PRECEDING;
                    	ancestor = tree.nodeParentNodeRepID[ancestor];
                    }
                    return DOCUMENT_POSITION_PRECEDING;
                } else {
                	int ancestor = tree.nodeParentNodeRepID[otherNode.node];
                    while (ancestor != Integer.MAX_VALUE && 
                    	   tree.nodeOrdinal[ancestor] <= tree.nodeOrdinal[otherNode.node]) {
                    	if (ancestor == node) 
                    		return DOCUMENT_POSITION_CONTAINED_BY | DOCUMENT_POSITION_FOLLOWING;
                    	ancestor = tree.nodeParentNodeRepID[ancestor];
                    }
                    return DOCUMENT_POSITION_FOLLOWING;
                }
            } else {
                return DOCUMENT_POSITION_DISCONNECTED;
            }
        } else {
            throw new DOMException(DOMException.NOT_SUPPORTED_ERR, null);
        }
    }


    public NamedNodeMap getAttributes() {
        return null;
    }

    public String getBaseURI() {
        return tree.getDocumentURI();
    }

    public NodeList getChildNodes() {
        return emptyNodeList;
    }

    /** Unsupported. */
    public Object getFeature(String feature, String version) {
        assert (false);
        return this;
    }

    public Node getFirstChild() {
        return null;
    }


    public Node getLastChild() {
        return null;
    }


    public String getLocalName() {
        return null;
    }


    public String getNamespaceURI() {
        return null;
    }

    protected Node getNextChild(int node) {
        return null;
    }


    public Node getNextSibling() {
        NodeImpl p = (NodeImpl)getParentNode();
        return (p == null ? null : p.getNextChild(node));
    }


    public abstract String getNodeName();


    public short getNodeType() {
        return NodeKind.domType(tree.nodeKind[node]);
    }


    public String getNodeValue() throws DOMException {
        return null; // overridden in some subclasses
    }

    public Document getOwnerDocument() {
        return (DocumentImpl)(this.tree.node(0));
    }


    public Node getParentNode() {
        //assume no linkNodeKind
        return tree.node(tree.nodeParentNodeRepID[node]);
    }

    protected int getPrefixID(int uriAtom) {
    	return -1;
    }

    public String getPrefix() {
        return null;
    }

    protected Node getPreviousChild(int child) {
        return null;
    }


    public Node getPreviousSibling() {
        NodeImpl p = (NodeImpl)getParentNode();
        return (p == null ? null : p.getPreviousChild(node));
    }

    // visit every child node, excluding COMMENT_NODE and
    // PROCESSING_INSTRUCTION_NODE nodes.
    private boolean hasTextContent(Node child) {
        return child.getNodeType() != Node.COMMENT_NODE
            && child.getNodeType() != Node.PROCESSING_INSTRUCTION_NODE;
    }
    
    // overwritten by some Text, Comment and PI
    public String getTextContent() throws DOMException {
        StringBuilder sb = new StringBuilder();
        getTextContent(sb);
        return sb.toString();
    }
    
    // internal method taking a StringBuffer in parameter
    private void getTextContent(StringBuilder sb) throws DOMException {
        NodeList children = getChildNodes();
        for(int i=0; i<children.getLength(); i++) {
            Node child = children.item(i);
            if(hasTextContent(child)) {
                sb.append(child.getTextContent());
            }
        }
    }

    /** Unsupported. */
    public Object getUserData(String key) {
        throw new DOMException(DOMException.NOT_SUPPORTED_ERR, null);
    }


    public boolean hasAttributes() {
        return false;
    }


    public boolean hasChildNodes() {
        return false;
    }


    /** Unsupported. */
    public Node insertBefore(Node newChild, Node refChild) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Not supported for namespace declaration.
     * Overrided by DocumentImpl and ElementImpl
     * </p>
     */
    public boolean isDefaultNamespace(String namespaceURI) {
        int type = getNodeType();
        if (type == NodeKind.ATTR) {
            if (this instanceof AttrImpl == false) {
                // ns decl
                throw new UnsupportedOperationException();
            }
        }
        Node p = getParentNode();
        return p.isDefaultNamespace(namespaceURI);
    }

    public boolean isEqualNode(Node other) {
    	
    	// Note that normalization can affect equality; to avoid this, 
    	// nodes should be normalized before being compared.
    	// For the moment, normalization cannot be done. 
    	
    	if (getNodeType() !=  other.getNodeType()) 		return false;
    	if (getLocalName() !=  other.getLocalName()) 	return false;
    	if (getNamespaceURI() !=  other.getNamespaceURI()) return false;
    	if (getPrefix() !=  other.getPrefix()) 			return false;
    	if (getNodeValue() !=  other.getNodeValue()) 	return false;
    	if (hasChildNodes() !=  other.hasChildNodes()) 	return false;
    	if (hasAttributes() !=  other.hasAttributes()) 	return false;
    	if (hasChildNodes()) {
    		NamedNodeMap thisAttr = getAttributes();
    		NamedNodeMap otherAttr = other.getAttributes();
    		if (thisAttr.getLength() != otherAttr.getLength()) return false;
    		for (int i = 0; i < thisAttr.getLength(); i++)
    			if (thisAttr.item(i).isEqualNode(otherAttr.item(i)))
    				return false;
    	}
    	if (hasAttributes()) {
    		NodeList thisChild = getChildNodes();
    		NodeList otherChild = other.getChildNodes();
    		if (thisChild.getLength() != otherChild.getLength()) return false;
    		for (int i = 0; i < thisChild.getLength(); i++)
    			if (thisChild.item(i).isEqualNode(otherChild.item(i)))
    				return false;
    	}
    	return true;
    }


    public boolean isSameNode(Node other) {
        return (other instanceof NodeImpl) && (((NodeImpl)other).tree == tree) && (((NodeImpl)other).node == node);
    }

    // TODO: Consider implementing Traversal
    // TODO - override in subclasses?

    public boolean isSupported(String feature, String version) {
        if (feature.equalsIgnoreCase("Core"))
            return true;
        if (feature.equalsIgnoreCase("XML"))
            return true;
        return false;
    }

    protected int getNSNodeID(long ordinal, long minOrdinal) {
        if (ordinal < minOrdinal)
            return -1;
        int idx = getNSNodeID(ordinal);
        if (idx >= 0 && tree.nsNodeOrdinal[idx] >= minOrdinal)
            return idx;
        return -1;
    }
    
    protected int getNSNodeID(long ordinal) {
        int R = tree.numNSNodeReps;
        if (R == 0) return -1;
        int L = 0;
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
            if (L == -1) 
            	break; 
            if (tree.nsNodePrefixAtom[L] != -1)
                break;
            L = tree.nsNodePrevNSNodeRepID[L];
        }
        return L;
    }
    
    protected int nextNSNodeID(int ns, long minOrdinal) {
    	for (;;) {
    		ns = tree.nsNodePrevNSNodeRepID[ns];
    		if ( ns == -1 ) return -1;
    		if ( tree.nsNodePrefixAtom[ns] != -1) break;
    	}
    	if ( tree.nsNodeOrdinal[ns] < minOrdinal ) ns = -1;
    	return ns;
    }
     
    // http://www.w3.org/TR/DOM-Level-3-Core/namespaces-algorithms.html#lookupNamespacePrefixAlgo

    public String lookupNamespaceURI(String prefix) {
        return null;
    }

    public String lookupPrefix(String namespaceURI) {
        return null;
    }


    /** Unsupported. */
    public void normalize() {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }


    /** Unsupported. */
    public Node removeChild(Node oldChild) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }


    /** Unsupported. */
    public Node replaceChild(Node newChild, Node oldChild) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }


    /** Unsupported. */
    public void setNodeValue(String nodeValue) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }


    /** Unsupported. */
    public void setPrefix(String prefix) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }


    /** Unsupported. */
    public void setTextContent(String textContent) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }


    /** Unsupported. */
    public Object setUserData(String key, Object data, UserDataHandler handler) {
        throw new DOMException(DOMException.NOT_SUPPORTED_ERR, null);
    }
    


	protected NodeList getElementsByTagNameNSOrNodeName(String namespaceURI, String name,final boolean nodeName) {
		
		final String tagname = name;
		final String ns = namespaceURI;
		final Node thisNode = this;
		
		return new NodeList() {
			protected ArrayList<Node> elementList = new ArrayList<Node>();
			protected boolean done = false;
			
			protected void init() {
				if (done) return;
				ArrayList<Node> childrenList = new ArrayList<Node>();
				childrenList.add(thisNode);
				int i = 0;
				while ( i != childrenList.size()) {
					Node curr = childrenList.get(i);
					NodeList children = curr.getChildNodes();
					for (int childi =0 ; childi < children.getLength(); childi ++)
						if (children.item(childi).getNodeType() == Node.ELEMENT_NODE)
							childrenList.add(children.item(childi));
					i ++;
					if (i == 1) continue; 
					if (nodeName) {
						if (curr.getNodeName().equals(tagname) || tagname.equals("*"))
							elementList.add(curr);
					}
					else {
						// do nothing if only one of the two is null
						if ("*".equals(ns) && "*".equals(tagname)){
							elementList.add(curr); continue;
						}
						if (ns != null) {
							if ((ns.equals("*") || ns.equals(curr.getNamespaceURI())) &&
								(tagname.equals("*") || tagname.equals(curr.getLocalName())))
								elementList.add(curr);
						}
						else if (tagname.equals("*") || tagname.equals(curr.getLocalName()))
								elementList.add(curr);
					}
				}
				done = true;
			}
			
			public int getLength() {
				init();
				return elementList.size();
			}

			public Node item(int index) {
				init();
				return (index < getLength()) ? elementList.get(index) : null;
			}
			
		};
	}
	
	protected String builtinNSPrefix(String URI) {
		if (URI == null) return null;
		if (URI.equals("http://www.w3.org/XML/1998/namespace")) return "xml";
		if (URI.equals("http://www.w3.org/2000/xmlns/")) return "xmlns";
		if (URI.equals("http://www.w3.org/2001/XMLSchema")) return "xs";
		if (URI.equals("http://www.w3.org/2001/XMLSchema-instance")) return "xsi";
		if (URI.equals("http://www.w3.org/2003/05/xpath-datatypes")) return "xdt";
		if (URI.equals("http://marklogic.com/xdmp")) return "xdmp";
		if (URI.equals("http://marklogic.com/xqe")) return "xqe";
		if (URI.equals("http://marklogic.com/xdmp/security")) return "sec";
		if (URI.equals("http://www.w3.org/2005/xqt-errors")) return "err";
		if (URI.equals("http://marklogic.com/xdmp/error")) return "error";
		if (URI.equals("http://marklogic.com/xdmp/directory")) return "dir";
		if (URI.equals("DAV:")) return "dav";
		if (URI.equals("http://marklogic.com/xdmp/lock")) return "lock";
		if (URI.equals("http://marklogic.com/xdmp/property")) return "prop";
		return null;
	}
}
