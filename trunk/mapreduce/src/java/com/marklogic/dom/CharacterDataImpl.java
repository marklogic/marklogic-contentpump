package com.marklogic.dom;

import org.w3c.dom.CharacterData;
import org.w3c.dom.DOMException;

import com.marklogic.tree.ExpandedTree;

public class CharacterDataImpl extends NodeImpl implements CharacterData {

    public CharacterDataImpl(ExpandedTree tree, int node) {
        super(tree, node);
    }

    @Override
    public void appendData(String arg) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    @Override
    public void deleteData(int offset, int count) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    @Override
    public String getData() throws DOMException {
        return tree.getText(tree.nodeRepID[node]);
    }

    @Override
    public int getLength() {
        return getData().length();
    }

    @Override
    public String getNodeValue() throws DOMException {
        return getData();
    }

    @Override
    public void insertData(int offset, String arg) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    @Override
    public void replaceData(int offset, int count, String arg) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    @Override
    public void setData(String data) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    @Override
    public String substringData(int offset, int count) throws DOMException {
    	if ((offset < 0) || (count < 0)) {
    		throw new DOMException(DOMException.INDEX_SIZE_ERR, null);
    	}
        String data = getData();
    	if (offset > data.length()) {
    		throw new DOMException(DOMException.INDEX_SIZE_ERR, null);
    	}
    	if (offset+count > data.length()) {
    		return data.substring(offset);
    	}
    	else {
            return data.substring(offset, offset+count);
    	}
    }
}
