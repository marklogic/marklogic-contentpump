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

import org.w3c.dom.CharacterData;
import org.w3c.dom.DOMException;

import com.marklogic.tree.ExpandedTree;

/**
 * A read-only W3C DOM Node implementation of MarkLogic's
 * internal representation of character data as stored in the expanded 
 * tree cache of a forest on disk. 
 * 
 * <p>
 * This interface is effectively read-only. Setters and update methods 
 * inherited from <code>org.w3c.Node</code> are not supported and will raise
 * an exception if called. To create a modifiable copy of a node, use
 * {@link #cloneNode}.
 * </p>
 * 
 * @author jchen
 */
public abstract class CharacterDataImpl extends NodeImpl implements CharacterData {

    public CharacterDataImpl(ExpandedTree tree, int node) {
        super(tree, node);
    }
    
    /** Unsupported. */
    public void appendData(String arg) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    public void deleteData(int offset, int count) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    public String getData() throws DOMException {
        return tree.getText(tree.nodeRepID[node]);
    }

    public int getLength() {
        return getData().length();
    }

    @Override
    public String getNodeValue() throws DOMException {
        return getData();
    }

    /** Unsupported. */
    public void insertData(int offset, String arg) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    public void replaceData(int offset, int count, String arg) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    public void setData(String data) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

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
