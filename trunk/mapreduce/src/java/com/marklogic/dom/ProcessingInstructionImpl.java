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

import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.ProcessingInstruction;

import com.marklogic.tree.ExpandedTree;

public class ProcessingInstructionImpl extends NodeImpl implements
		ProcessingInstruction {

	public ProcessingInstructionImpl(ExpandedTree tree, int node) {
		super(tree, node);
	}

    public Node cloneNode(Document doc, boolean deep) {
        return doc.createProcessingInstruction(getTarget(), getData());
    }
    
	public String getData() {
	    return tree.getText(tree.piNodeTextRepID[tree.nodeRepID[node]]);
	}

	@Override
	public String getNodeName() {
		return getTarget();
	}

	@Override
	public String getNodeValue() {
		return getData();
	}

	public String getTarget() {
		return tree.atomString(tree.piNodeTargetAtom[tree.nodeRepID[node]]);
	}

    @Override
    public String getTextContent() throws DOMException {
        return getNodeValue();
    }

	public void setData(String data) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}
}
