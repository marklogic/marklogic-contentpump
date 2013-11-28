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
