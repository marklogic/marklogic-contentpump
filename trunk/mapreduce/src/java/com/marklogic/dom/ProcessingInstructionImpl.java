package com.marklogic.dom;

import org.w3c.dom.DOMException;
import org.w3c.dom.ProcessingInstruction;

import com.marklogic.tree.ExpandedTree;

public class ProcessingInstructionImpl extends NodeImpl implements
		ProcessingInstruction {

	public ProcessingInstructionImpl(ExpandedTree tree, int node) {
		super(tree, node);
	}

	@Override
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

	@Override
	public String getTarget() {
		return tree.atomString(tree.piNodeTargetAtom[tree.nodeRepID[node]]);
	}

    @Override
    public String getTextContent() throws DOMException {
        return getNodeValue();
    }

	@Override
	public void setData(String data) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR, null);
	}
}
