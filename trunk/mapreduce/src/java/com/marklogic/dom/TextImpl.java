package com.marklogic.dom;

import org.w3c.dom.DOMException;
import org.w3c.dom.Text;

import com.marklogic.tree.ExpandedTree;

public class TextImpl extends CharacterDataImpl implements Text {

	public TextImpl(ExpandedTree tree, int node) {
		super(tree, node);
	}

	@Override
	public String getNodeName() {
		return "#text";
	}

	@Override
    public String getTextContent() throws DOMException {
        return getNodeValue();
    }

	@Override
	public String getWholeText() {
		// TODO Auto-generated method stub
		return null;
	}

    @Override
	public boolean isElementContentWhitespace() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Text replaceWholeText(String content) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}

	@Override
	public Text splitText(int offset) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}
}
