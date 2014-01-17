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

import org.w3c.dom.Comment;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import com.marklogic.tree.ExpandedTree;

/**
 * A read-only W3C DOM Node implementation of MarkLogic's
 * internal representation of an XML comment as stored in the expanded 
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
public class CommentImpl extends CharacterDataImpl implements Comment {

	public CommentImpl(ExpandedTree tree, int node) {
		super(tree, node);
	}
	
    public Node cloneNode(Document doc, boolean deep) {
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
