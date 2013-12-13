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
import org.w3c.dom.CDATASection;
import org.w3c.dom.Comment;
import org.w3c.dom.DOMConfiguration;
import org.w3c.dom.DOMException;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.DocumentType;
import org.w3c.dom.Element;
import org.w3c.dom.EntityReference;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.ProcessingInstruction;
import org.w3c.dom.Text;

import com.marklogic.tree.ExpandedTree;

public class DocumentImpl extends NodeImpl implements Document {

	public DocumentImpl(ExpandedTree tree, int node) {
		super(tree, node);
	}

	@Override
	public String getNodeName() {
		return "#document";
	}

	protected int getNumChildren() {
		return tree.docNodeNumChildren[tree.nodeRepID[node]];
	}

	public int getFirstChildIndex() {
		return tree.docNodeChildNodeRepID[tree.nodeRepID[node]];
	}

	public NodeList getChildNodes() {
		return new NodeList() {
			@Override
			public int getLength() {
				return getNumChildren();
			}

			@Override
			public Node item(int index) {
				return (index < getNumChildren()) ? tree
						.node(getFirstChildIndex() + index) : null;
			}
		};
	}

	public Node getFirstChild() {
		int i = getFirstChildIndex();
		return (i != Integer.MAX_VALUE) ? tree.node(i) : null;
	}

	public Node getLastChild() {
		int i = tree.docNodeChildNodeRepID[tree.nodeRepID[node]];
		return (i != Integer.MAX_VALUE) ? tree.node(i
				+ tree.docNodeNumChildren[node] - 1) : null;
	}

	public boolean hasChildNodes() {
		return (getFirstChildIndex() != Integer.MAX_VALUE);
	}

	protected Node getNextChild(int child) {
		if (child - getFirstChildIndex() + 1 < getNumChildren()) {
			return tree.node(child + 1);
		} else {
			return null;
		}
	}

	protected Node getPreviousChild(int node) {
		if (node != getFirstChildIndex()) {
			return tree.node(node - 1);
		} else {
			return null;
		}
	}

	@Override
	public String getTextContent() throws DOMException {
		return null;
	}

	public Node adoptNode(Node arg0) throws DOMException {
		// TODO Auto-generated method stub
		return null;
	}

	public Attr createAttribute(String arg0) throws DOMException {
		// TODO Auto-generated method stub
		return null;
	}

	public Attr createAttributeNS(String arg0, String arg1) throws DOMException {
		// TODO Auto-generated method stub
		return null;
	}

	public CDATASection createCDATASection(String arg0) throws DOMException {
		// TODO Auto-generated method stub
		return null;
	}

	public Comment createComment(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	public DocumentFragment createDocumentFragment() {
		// TODO Auto-generated method stub
		return null;
	}

	public Element createElement(String arg0) throws DOMException {
		// TODO Auto-generated method stub
		return null;
	}

	public Element createElementNS(String arg0, String arg1)
			throws DOMException {
		// TODO Auto-generated method stub
		return null;
	}

	public EntityReference createEntityReference(String arg0)
			throws DOMException {
		// TODO Auto-generated method stub
		return null;
	}

	public ProcessingInstruction createProcessingInstruction(String arg0,
			String arg1) throws DOMException {
		// TODO Auto-generated method stub
		return null;
	}

	public Text createTextNode(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	public DocumentType getDoctype() {
		// TODO Auto-generated method stub
		return null;
	}

	public Element getDocumentElement() {
		// TODO Auto-generated method stub
		return null;
	}

	public String getDocumentURI() {
		return tree.getDocumentURI();
	}

	public DOMConfiguration getDomConfig() {
		// TODO Auto-generated method stub
		return null;
	}

	public Element getElementById(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	public NodeList getElementsByTagName(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	public NodeList getElementsByTagNameNS(String arg0, String arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	public DOMImplementation getImplementation() {
		// TODO Auto-generated method stub
		return null;
	}

	public String getInputEncoding() {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean getStrictErrorChecking() {
		// TODO Auto-generated method stub
		return false;
	}

	public String getXmlEncoding() {
		return null;
	}

	public boolean getXmlStandalone() {
		return true;
	}

	public String getXmlVersion() {
		return "1.0";
	}

	public Node importNode(Node arg0, boolean arg1) throws DOMException {
		// TODO Auto-generated method stub
		return null;
	}

	public void normalizeDocument() {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}

	public Node renameNode(Node arg0, String arg1, String arg2)
			throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}

	public void setDocumentURI(String arg0) {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}

	public void setStrictErrorChecking(boolean arg0) {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}

	public void setXmlStandalone(boolean arg0) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}

	public void setXmlVersion(String arg0) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}

}
