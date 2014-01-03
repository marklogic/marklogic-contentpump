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

import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Comment;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import com.marklogic.tree.ExpandedTree;

public class CommentImpl extends CharacterDataImpl implements Comment {

	public CommentImpl(ExpandedTree tree, int node) {
		super(tree, node);
	}
	
    public Node cloneNode(boolean deep) {
        Document doc;
        try {
            doc = tree.getClonedDocOwner();
        } catch (ParserConfigurationException e) {
            throw new RuntimeException("Internal Error:" + e);
        }
        return doc.createComment(getData());
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
