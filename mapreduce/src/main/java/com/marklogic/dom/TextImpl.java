/*
 * Copyright 2003-2015 MarkLogic Corporation
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
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.Text;

import com.marklogic.tree.ExpandedTree;

/**
 * A read-only W3C DOM Node implementation of MarkLogic's internal
 * representation of text data as stored in the expanded tree cache of a forest
 * on disk.
 * 
 * <p>
 * This interface is effectively read-only: Setters and update methods inherited
 * from <code>org.w3c.Node</code> are not supported and will raise an exception
 * if called.
 * </p>
 * 
 * @author jchen
 */
public class TextImpl extends CharacterDataImpl implements Text {

    public TextImpl(ExpandedTree tree, int node) {
        super(tree, node);
    }

    protected Node cloneNode(Document doc, boolean deep) {
        return doc.createTextNode(getNodeValue());
    }

    @Override
    public String getNodeName() {
        return "#text";
    }

    @Override
    public String getTextContent() throws DOMException {
        return getNodeValue();
    }

    /** Unsupported. */
    public String getWholeText() {
        return null;
    }

    /** Unsupported. */
    public boolean isElementContentWhitespace() {
        return false;
    }

    /** Unsupported. */
    public Text replaceWholeText(String content) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }

    /** Unsupported. */
    public Text splitText(int offset) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
    }
}
