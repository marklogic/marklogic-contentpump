package com.marklogic.dom;

import org.w3c.dom.Comment;
import org.w3c.dom.DOMException;

import com.marklogic.tree.ExpandedTree;

public class CommentImpl extends CharacterDataImpl implements Comment {

	public CommentImpl(ExpandedTree tree, int node) {
		super(tree, node);
	}
	
	@Override
	public String getNodeName() {
		return "#comment";
	}

	@Override
    public String getTextContent() throws DOMException {
        return getNodeValue();
    }
}
